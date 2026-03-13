import asyncio
import io
import logging
import multiprocessing
import time
import uuid
import os
from typing import Optional, Union, List, Any
from urllib.parse import urlparse

from media_toolkit import MediaFile, IMediaContainer
from media_toolkit.utils.dependency_requirements import requires

# FastCloud base class handles all type-dispatch logic (dict, list, single file)
# so S3Storage only needs to implement the raw file-level operations.
from fastCloud.core.i_fast_cloud import FastCloud

try:
    import boto3
    from boto3 import session
    from boto3.s3.transfer import TransferConfig
    from botocore.config import Config
    from botocore.exceptions import ClientError
except ImportError:
    pass

try:
    import aioboto3
except ImportError:
    pass


class S3Storage(FastCloud):
    """
    S3-compatible cloud storage provider (AWS S3, DigitalOcean Spaces, Scaleway, MinIO, ...).

    Follows the same architecture as AzureBlobStorage:
      - Extends FastCloud so upload() / upload_async() handle all type dispatching
        (single file, list, dict) before delegating to _upload_files / _upload_files_async.
      - Only the file-level primitives (_upload_files, _upload_files_async, download, delete)
        need to be implemented here.

    Generic usage:
        storage = S3Storage(
            endpoint_url="https://s3.nl-ams.scw.cloud",
            access_key_id="...",
            access_key_secret="...",
        )
        url  = storage.upload(my_media_file, folder="my-bucket")
        urls = await storage.upload_async([file1, file2], folder="my-bucket")

    Scaleway shortcut (recommended):
        storage = S3Storage.from_scaleway(
            access_key_id="...",
            access_key_secret="...",
            region="fr-par",        # Paris — default
        )
    """

    # All Scaleway Object Storage regions — all three are EU-based.
    # Docs: https://www.scaleway.com/en/docs/storage/object/concepts/#region
    SCALEWAY_REGIONS = {
        "fr-par": "Paris, France (EU)",
        "nl-ams": "Amsterdam, Netherlands (EU)",
        "pl-waw": "Warsaw, Poland (EU)",
    }
    _SCALEWAY_ENDPOINT_TEMPLATE = "https://s3.{region}.scw.cloud"

    @requires("boto3")
    def __init__(
        self,
        endpoint_url: str = None,
        access_key_id: str = None,
        access_key_secret: str = None,
    ):
        """
        Initialise the S3Storage client.

        :param endpoint_url:      S3-compatible endpoint, e.g. "https://nyc3.digitaloceanspaces.com"
        :param access_key_id:     AWS / provider access key ID.
        :param access_key_secret: AWS / provider secret access key.
        """
        self.endpoint_url = endpoint_url
        self.access_key_id = access_key_id
        self.secret_access_key = access_key_secret

        # Multipart upload configuration — mirrors what the original implementation used.
        self.transfer_config = TransferConfig(
            multipart_threshold=1024 * 25,   # 25 MB threshold before switching to multipart
            max_concurrency=multiprocessing.cpu_count(),
            multipart_chunksize=1024 * 25,
            use_threads=True
        )

        # Lazily initialised sync boto3 client (one per instance, thread-safe for reads).
        self._boto_client = None

        # Lazily initialised aioboto3 Session for async operations.
        # We cache the *Session* rather than the client because aioboto3 clients
        # must be used as async context managers and cannot be kept open persistently.
        # Creating a new Session is cheap — it holds no network connections itself.
        self._aioboto_session = None

        # Suppress overly verbose boto3 / botocore logs (mirrors Azure's approach).
        logging.getLogger("boto3").setLevel(logging.ERROR)
        logging.getLogger("botocore").setLevel(logging.ERROR)
        logging.getLogger("aioboto3").setLevel(logging.ERROR)
        logging.getLogger("aiobotocore").setLevel(logging.ERROR)


    @classmethod
    def from_scaleway(
        cls,
        access_key_id: str,
        access_key_secret: str,
        region: str = "fr-par",
    ) -> "S3Storage":
        """
        Factory method for Scaleway Object Storage — the recommended entry point
        when working with Scaleway instead of configuring the endpoint manually.

        Scaleway's S3-compatible endpoint pattern:
            https://s3.<region>.scw.cloud

        All three available regions are EU-based (GDPR-friendly):
            "fr-par"  -> Paris, France        (default)
            "nl-ams"  -> Amsterdam, Netherlands
            "pl-waw"  -> Warsaw, Poland

        Example:
            storage = S3Storage.from_scaleway(
                access_key_id="SCW...",
                access_key_secret="...",
                region="fr-par",
            )
            url = storage.upload(my_file, folder="my-bucket")

        :param access_key_id:     Scaleway access key (starts with "SCW").
        :param access_key_secret: Scaleway secret key.
        :param region:            Region slug. Defaults to "fr-par" (Paris, EU).
        :raises ValueError:       On unrecognised region slug.
        """
        if region not in cls.SCALEWAY_REGIONS:
            raise ValueError(
                f"Unknown Scaleway region {region!r}. "
                f"Valid options: {list(cls.SCALEWAY_REGIONS.keys())}"
            )
        endpoint_url = cls._SCALEWAY_ENDPOINT_TEMPLATE.format(region=region)
        logging.getLogger(__name__).info(
            "Initialising Scaleway S3Storage | region=%s (%s) | endpoint=%s",
            region,
            cls.SCALEWAY_REGIONS[region],
            endpoint_url,
        )
        return cls(
            endpoint_url=endpoint_url,
            access_key_id=access_key_id,
            access_key_secret=access_key_secret,
        )

    # ------------------------------------------------------------------ #
    # Internal client management                                           #
    # ------------------------------------------------------------------ #


    def _get_boto_client(self) -> "boto3.client":
        """
        Return a cached boto3 S3 client, creating one on first call.

        Keeping a single client instance avoids the overhead of re-authenticating
        on every request (same pattern as AzureBlobStorage._get_blob_service_client).
        """
        if self._boto_client is not None:
            return self._boto_client

        if not all([self.endpoint_url, self.access_key_id, self.secret_access_key]):
            raise ValueError(
                "endpoint_url, access_key_id, and access_key_secret are all required."
            )

        region = self._extract_region_from_url(self.endpoint_url)
        boto_config = Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
        )
        self._boto_client = session.Session().client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            config=boto_config,
            region_name=region,
        )
        return self._boto_client

    @requires("aioboto3")
    def _get_aioboto_client_context(self):
        """
        Return an aioboto3 S3 client as an *async context manager*.

        Why a context manager instead of a cached client?
        aioboto3 clients own an aiohttp connection pool that must be explicitly
        closed to avoid resource leaks. The async-with pattern guarantees cleanup
        even when exceptions occur.  We build the client config fresh each time,
        but the underlying aioboto3 Session (and its resolver cache) is reused
        across calls via self._aioboto_session.

        Typical usage:
            async with self._get_aioboto_client_context() as client:
                await client.upload_fileobj(...)
        """
        if self._aioboto_session is None:
            self._aioboto_session = aioboto3.Session()

        region = self._extract_region_from_url(self.endpoint_url)
        boto_config = Config(
            signature_version="s3v4",
            retries={"max_attempts": 3, "mode": "standard"},
        )
        # Returns the async context manager — caller must use `async with`.
        return self._aioboto_session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            config=boto_config,
            region_name=region,
        )

    # ------------------------------------------------------------------ #
    # Core upload primitives (called by FastCloud after type dispatch)     #
    # ------------------------------------------------------------------ #

    def _upload_files(
        self,
        files: Union[MediaFile, List[MediaFile]],
        folder: str,
        *args,
        **kwargs,
    ) -> Union[str, List[str]]:
        """
        Upload one or more MediaFile objects to S3 synchronously.

        FastCloud guarantees that `files` is already a list of MediaFile by the
        time it reaches here, so we don't need to repeat type-checking.

        :param files:  A single MediaFile or a list of MediaFile instances.
        :param folder: The S3 bucket name (equivalent to Azure's container).
        :return:       A single URL string when one file was uploaded, or a list of URLs.
        """
        if not isinstance(files, list):
            files = [files]

        boto_client = self._get_boto_client()
        urls: List[str] = []

        for f in files:
            # Guard 1: FastCloud's get_processable_files() may hand us raw bytes,
            # file paths, or other non-MediaFile types depending on the container
            # variant (MediaList, MediaDict, nested structures). Normalise everything
            # to MediaFile so the rest of this loop is type-safe.
            if not isinstance(f, MediaFile):
                f = MediaFile().from_any(f)

            # Guard 2: Certain MediaFile subclasses (e.g. VideoFile) implement
            # to_bytes() as a generator/chunked API that returns a list of byte
            # segments rather than a single bytes object. This is transparent in
            # the sync path because FastCloud recurses item-by-item, but surfaces
            # in the async path where all files arrive together via
            # get_processable_files(). Join the chunks into a single bytes object
            # before handing off to BytesIO.
            raw = f.to_bytes()
            if isinstance(raw, (list, tuple)):
                raw = b"".join(raw)

            # Determine the S3 object key (the filename stored in the bucket).
            #
            # MediaFile._file_info() guarantees file_name is never None — it
            # always falls back to the literal string "file". We treat "file"
            # as "no meaningful name" and generate a UUID instead.
            #
            # Crucially we preserve the file extension by reading MediaFile.extension,
            # which first maps content_type via mime_to_extension(), then falls back
            # to the filename suffix. Without this, files uploaded with a UUID key
            # would have NO extension and S3 would serve them as application/octet-stream
            # regardless of their actual type — breaking any client that relies on
            # the Content-Type header or the key suffix to determine how to handle them.
            #
            # Examples:
            #   VideoFile  -> content_type="video/mp4"  -> extension="mp4"  -> key="<uuid>.mp4"
            #   ImageFile  -> content_type="image/png"  -> extension="png"  -> key="<uuid>.png"
            #   MediaFile with file_name="report.pdf"   ->                     key="report.pdf"
            if not f.file_name or f.file_name in ("", "file"):
                ext = f.extension  # None when truly undetermined
                base = str(uuid.uuid4())
                f.file_name = f"{base}.{ext}" if ext else base

            key = f.file_name

            # content_type is guaranteed non-None by MediaFile._file_info()
            # (defaults to "application/octet-stream"), so the fallback here
            # is purely defensive for any non-standard MediaFile subclass.
            content_type = f.content_type or "application/octet-stream"

            boto_client.upload_fileobj(
                io.BytesIO(raw),
                folder,
                key,
                ExtraArgs={
                    "ContentType": content_type,
                    "ACL": "public-read",
                },
                Config=self.transfer_config,
            )

            # Build a public URL — mirrors how Azure returns blob_client.url.
            url = f"{self.endpoint_url.rstrip('/')}/{folder}/{key}"
            urls.append(url)

        return urls[0] if len(urls) == 1 else urls

    async def _upload_files_async(
        self,
        files: Union[MediaFile, List[MediaFile]],
        folder: str,
        *args,
        **kwargs,
    ) -> Union[str, List[str]]:
        """
        Upload one or more MediaFile objects to S3 using true async I/O via aioboto3.

        Design rationale vs the old asyncio.to_thread approach:
          - asyncio.to_thread offloads the synchronous boto3 call to a thread pool.
            Uploads are still blocking at the OS level and contend over the GIL
            during serialisation; each upload consumes an OS thread.
          - aioboto3 (backed by aiohttp + aiobotocore) performs genuine async I/O.
            All uploads share a single event loop with no thread overhead, and
            asyncio.gather runs them concurrently within a single client context.

        We deliberately prepare all files (CPU-bound: type coercion, byte
        serialisation) BEFORE opening the async client so the heavy work doesn't
        block inside the async context where we want pure I/O.

        :param files:  A single MediaFile or a list of MediaFile instances.
        :param folder: The S3 bucket name.
        :return:       A single URL string when one file was uploaded, or a list of URLs.
        """
        if not isinstance(files, list):
            files = [files]

        # --- Phase 1: prepare (CPU-bound, runs synchronously before async I/O) ---
        # Normalise types, join chunked bytes, and resolve filenames up front.
        # This keeps the async section below pure I/O with no heavy computation.
        prepared: List[tuple] = []
        for f in files:
            if not isinstance(f, MediaFile):
                f = MediaFile().from_any(f)

            raw = f.to_bytes()
            if isinstance(raw, (list, tuple)):
                raw = b"".join(raw)

            if not f.file_name or f.file_name in ("", "file"):
                ext = f.extension
                base = str(uuid.uuid4())
                f.file_name = f"{base}.{ext}" if ext else base

            prepared.append((f, raw))

        # --- Phase 2: upload (true async I/O, all files concurrent) -------------
        # Open a single aioboto3 client for the entire batch. The context manager
        # guarantees the underlying aiohttp session is properly closed afterwards,
        # even if one of the uploads raises an exception.
        async with self._get_aioboto_client_context() as client:
            tasks = [
                self._upload_single_file_async(client, f, raw, folder)
                for f, raw in prepared
            ]
            # gather() runs all uploads concurrently on the event loop.
            urls = await asyncio.gather(*tasks)

        return urls[0] if len(urls) == 1 else list(urls)

    async def _upload_single_file_async(
        self,
        client,
        f: MediaFile,
        raw: bytes,
        folder: str,
    ) -> str:
        """
        Upload a single pre-prepared MediaFile using an open aioboto3 client.

        Kept as a small, focused coroutine so _upload_files_async can fan out
        multiple of these with asyncio.gather cleanly.

        :param client: Active aioboto3 S3 client (open async context manager).
        :param f:      MediaFile with file_name and content_type already resolved.
        :param raw:    File contents as a single bytes object.
        :param folder: S3 bucket name.
        :return:       Public URL of the uploaded object.
        """
        await client.upload_fileobj(
            io.BytesIO(raw),
            folder,
            f.file_name,
            ExtraArgs={
                "ContentType": f.content_type or "application/octet-stream",
                "ACL": "public-read",
            },
        )
        return f"{self.endpoint_url.rstrip('/')}/{folder}/{f.file_name}"

    # ------------------------------------------------------------------ #
    # Public upload interface (delegates type-dispatch to FastCloud)       #
    # ------------------------------------------------------------------ #

    def upload(
        self,
        file: Union[IMediaContainer, MediaFile, Any],
        folder: str = None,
        *args,
        **kwargs,
    ) -> Union[str, List[str], dict]:
        """
        Upload one or more files to S3.

        Mirrors AzureBlobStorage.upload — validates the required `folder` argument
        then hands off to FastCloud.upload which handles dict / list / single-file
        dispatch before calling _upload_files.

        :param file:   The file(s) to upload. Accepts any type supported by media_toolkit.
        :param folder: S3 bucket name (required).
        :return:
            str            — if a single file was uploaded.
            List[str]      — if a list of files was uploaded.
            Dict[str, str] — if a dict of files was uploaded.
        """
        if folder is None:
            raise ValueError("folder (bucket name) must be provided for S3 upload.")

        kwargs["folder"] = kwargs.get("folder") or folder
        return super().upload(file, *args, **kwargs)

    async def upload_async(
        self,
        file: Union[IMediaContainer, MediaFile, Any],
        folder: str = None,
        *args,
        **kwargs,
    ) -> Union[str, List[str], dict]:
        """
        Upload one or more files to S3 asynchronously.

        :param file:   The file(s) to upload.
        :param folder: S3 bucket name (required).
        :return:       URL, list of URLs, or dict of URLs matching the input shape.
        """
        if folder is None:
            raise ValueError("folder (bucket name) must be provided for S3 async upload.")

        kwargs["folder"] = kwargs.get("folder") or folder
        return await super().upload_async(file, *args, **kwargs)

    # ------------------------------------------------------------------ #
    # Download                                                             #
    # ------------------------------------------------------------------ #

    def download(self, url: str, save_path: str = None, *args, **kwargs) -> Union[MediaFile, str, None]:
        """
        Download a blob from S3.

        :param url:       Full URL of the S3 object.
        :param save_path: Optional local path to write the file to.
                          When omitted the file is returned as a MediaFile in memory.
        :return:          MediaFile (in-memory) or the save_path string, mirroring Azure.
        """
        boto_client = self._get_boto_client()
        bucket, key = self._parse_s3_url(url)

        if save_path is None:
            buffer = io.BytesIO()
            boto_client.download_fileobj(bucket, key, buffer)
            buffer.seek(0)
            return MediaFile().from_any(buffer.read())

        boto_client.download_file(bucket, key, save_path)
        return save_path

    async def download_async(self, url: str, save_path: str = None, *args, **kwargs) -> Union[MediaFile, str, None]:
        """
        Download an S3 object asynchronously using aioboto3.

        :param url:       Full URL of the S3 object.
        :param save_path: Optional local path. When omitted returns a MediaFile.
        :return:          MediaFile (in-memory) or save_path string.
        """
        bucket, key = self._parse_s3_url(url)

        async with self._get_aioboto_client_context() as client:
            if save_path is None:
                buffer = io.BytesIO()
                await client.download_fileobj(bucket, key, buffer)
                buffer.seek(0)
                return MediaFile().from_any(buffer.read())

            await client.download_file(bucket, key, save_path)
            return save_path

    # ------------------------------------------------------------------ #
    # Delete                                                               #
    # ------------------------------------------------------------------ #

    def _delete_single_blob_sync(self, url: str) -> bool:
        """
        Delete a single S3 object synchronously.

        Follows the same helper-method pattern as AzureBlobStorage._delete_single_blob_sync,
        making the logic easy to test in isolation.
        """
        try:
            bucket, key = self._parse_s3_url(url)
            self._get_boto_client().delete_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            print(f"ClientError deleting {url}: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error deleting {url}: {e}")
            return False

    async def _delete_single_blob_async(self, url: str) -> bool:
        """
        Delete a single S3 object asynchronously using aioboto3.

        :param url: Full URL of the S3 object to delete.
        :return:    True on success, False on any error.
        """
        try:
            bucket, key = self._parse_s3_url(url)
            async with self._get_aioboto_client_context() as client:
                await client.delete_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            print(f"ClientError deleting {url}: {e}")
            return False
        except Exception as e:
            print(f"Unexpected error deleting {url}: {e}")
            return False

    def delete(self, url: Union[str, List[str]], *args, **kwargs) -> Union[bool, List[bool]]:
        """
        Delete one or more S3 objects synchronously.

        :param url: A single URL or a list of URLs to delete.
        :return:    bool for a single URL, List[bool] for a list — same shape as input.
        """
        if not url:
            return False

        if isinstance(url, str):
            return self._delete_single_blob_sync(url)

        if isinstance(url, list):
            # Deduplicate to avoid double-deleting the same object (mirrors Azure).
            return [self._delete_single_blob_sync(u) for u in set(url)]

        return False

    async def delete_async(self, url: Union[str, List[str]], *args, **kwargs) -> Union[bool, List[bool]]:
        """
        Delete one or more S3 objects asynchronously.

        Uses asyncio.gather so all deletions run concurrently, matching the
        parallel behaviour in AzureBlobStorage.delete_async.
        """
        if not url:
            return False

        if isinstance(url, str):
            return await self._delete_single_blob_async(url)

        if isinstance(url, list):
            tasks = [self._delete_single_blob_async(u) for u in set(url)]
            return list(await asyncio.gather(*tasks))

        return False

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    def _parse_s3_url(self, url: str) -> tuple[str, str]:
        """
        Extract (bucket, key) from an S3-compatible URL.

        Handles two URL styles used by different providers:

        Path-style (AWS default, MinIO):
            https://s3.eu-west-1.amazonaws.com/<bucket>/<key>
            -> first path segment = bucket, remainder = key

        Virtual-hosted style (Scaleway, DigitalOcean):
            https://<account>.s3.<region>.scw.cloud/<bucket>/<key>
            -> the account name lives in the subdomain (not the bucket);
               bucket and key are still in the path, same split as path-style.

        Because Scaleway embeds the account namespace in the subdomain and
        keeps bucket + key in the URL path, the path-split logic is identical
        for both styles — we just need to be explicit about it so future
        maintainers don't accidentally "fix" the working behaviour.

        Presigned URLs may carry a query string (?X-Amz-Signature=...) which
        is stripped before returning the key.

        :raises ValueError: When bucket or key cannot be extracted.
        """
        parsed = urlparse(url)
        # Split at the first "/" so that keys with slashes (folder/file.ext)
        # are preserved in full as a single string.
        path_parts = parsed.path.lstrip("/").split("/", 1)

        if len(path_parts) < 2 or not path_parts[1]:
            raise ValueError(
                f"Cannot extract bucket/key from URL {url!r}. "
                "Expected path format: /<bucket>/<key>"
            )

        bucket = path_parts[0]
        # Strip query string that presigned URLs append after the key.
        key = path_parts[1].split("?")[0]
        return bucket, key

    @staticmethod
    def _extract_region_from_url(endpoint_url: str) -> str:
        """
        Extract the AWS-style region string from a provider endpoint URL.

        Recognised patterns (checked in order):

        1. AWS standard / path-style:
               https://s3.<region>.amazonaws.com
               https://<bucket>.s3.<region>.amazonaws.com
           -> split on ".s3." and take the first subdomain of the remainder.

        2. Scaleway Object Storage:
               https://s3.<region>.scw.cloud          (path-style, recommended)
               https://<account>.s3.<region>.scw.cloud (virtual-hosted)
           -> split on ".s3." and take the first subdomain of the remainder.
           Scaleway region slugs: "fr-par", "nl-ams", "pl-waw"

        3. DigitalOcean Spaces:
               https://<region>.digitaloceanspaces.com
           -> the first hostname label is the region.

        4. Fallback:
           Returns "us-east-1" — boto3's default, accepted silently by most
           non-AWS providers that don't validate the region header.

        :param endpoint_url: Full HTTPS endpoint URL.
        :returns:            Region string, e.g. "fr-par" or "us-east-1".
        """
        parsed = urlparse(endpoint_url)
        hostname = parsed.netloc  # e.g. "socaity.s3.fr-par.scw.cloud"

        # AWS and Scaleway both embed the region after ".s3." in the hostname.
        # Pattern: [<prefix>.]s3.<region>.<tld>
        if ".s3." in hostname:
            # Everything after the first ".s3." -> "<region>.<rest>"
            after_s3 = hostname.split(".s3.", 1)[1]   # "fr-par.scw.cloud"
            region = after_s3.split(".")[0]            # "fr-par"
            if region:
                return region

        # DigitalOcean Spaces: <region>.digitaloceanspaces.com
        if hostname.endswith(".digitaloceanspaces.com"):
            return hostname.split(".digitaloceanspaces.com")[0].split(".")[-1]

        # Generic fallback — accepted by most non-AWS providers.
        return "us-east-1"