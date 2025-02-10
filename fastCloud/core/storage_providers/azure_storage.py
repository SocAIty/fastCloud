import logging
import uuid
from asyncio import gather
from datetime import datetime, timedelta
from typing import Union, List
import io
from urllib.parse import urlparse

from fastCloud.core.storage_providers.i_cloud_storage import CloudStorage

try:
    from azure.core.exceptions import ResourceNotFoundError
    from azure.storage.blob import BlobServiceClient
    from azure.storage.blob.aio import BlobServiceClient as AioBlobServiceClient
    from azure.storage.blob import generate_blob_sas, BlobSasPermissions
except ImportError:
    pass

try:
    import httpx
except:
    pass


from media_toolkit import MediaFile
from media_toolkit.utils.dependency_requirements import requires


class AzureBlobStorage(CloudStorage):
    @requires("azure.storage.blob")
    def __init__(self, sas_access_token: str = None, connection_string: str = None):
        """
        Create an azure blob storage client either with a SAS access token or a connection string.
        :param sas_access_token: sas_access_token in form
        :param connection_string: formatted like
        """
        if not sas_access_token and not connection_string:
            raise ValueError("Either a sas_access_token or a connection_string must be provided")

        self.sas_access_token = sas_access_token
        self.connection_string = connection_string

        self._blob_client = None
        self._async_blob_client = None

        # changing logging level of azure.storage.blob to print only errors
        self._blob_service_logger = logging.getLogger('azure.storage.blob')
        self._blob_service_logger.setLevel(logging.ERROR)

    def _get_blob_service_client(self, async_mode: bool = False):
        """Retrieve or create a BlobServiceClient."""
        if self.sas_access_token:
            if async_mode:
                if not self._async_blob_client:
                    self._async_blob_client = AioBlobServiceClient(account_url=self.sas_access_token)
                return self._async_blob_client

            if not self._blob_client:
                self._blob_client = BlobServiceClient(account_url=self.sas_access_token)
            return self._blob_client

        elif self.connection_string:
            if async_mode:
                if not self._async_blob_client:
                    self._async_blob_client = AioBlobServiceClient.from_connection_string(self.connection_string)
                return self._async_blob_client

            if not self._blob_client:
                self._blob_client = BlobServiceClient.from_connection_string(self.connection_string)
            return self._blob_client

    def upload(
            self,
            file: Union[bytes, io.BytesIO, MediaFile, str, list],
            file_name: Union[str, list] = None,
            folder: str = None
    ) -> Union[str, List[str]]:
        """
        Upload a file to Azure Blob Storage.

        Args:
            file: The file(s) to upload - can be bytes, BytesIO, MediaFile, or a file path string
            file_name: Optional name(s) for the file. If None, a UUID will be generated for each one.
            folder: The container name in Azure Blob Storage (required)

        Returns:
            str: The URL(s) of the uploaded blob(s)

        Raises:
            ValueError: If folder (container name) is not provided
        """
        if folder is None:
            raise ValueError("Folder aka container name must be provided for Azure Blob upload")

        if not isinstance(file, list):
            file = [file]

        file = [MediaFile().from_any(f) for f in file]

        # make sure file_name is a list
        if not isinstance(file_name, list):
            file_name = [file_name]

        # safety check
        # ToDo: Add extensions depending on file type

        # replace None with UUID
        file_name = [
            f if f is not None and isinstance(f, str) else str(uuid.uuid4())
            for f in file_name
        ]
        # make file_name list have the same length as file(s)
        # if too less entries fill with uuids if too many entries ignore additional ones
        if len(file_name) != len(file):
            file_name = [
                file_name[i] if i < len(file_name) else str(uuid.uuid4())
                for i in range(len(file))
            ]

        blob_service_client = self._get_blob_service_client(async_mode=False)

        urls = []
        for f, fn in zip(file, file_name):
            blob_client = blob_service_client.get_blob_client(container=folder, blob=file_name[0])

            b = f.to_bytes()
            blob_client.upload_blob(b, overwrite=True)
            urls.append(blob_client.url)

        if len(urls) == 1:
            return urls[0]

        return urls

    async def upload_async(
            self,
            file: Union[bytes, io.BytesIO, MediaFile, str, list],
            file_name: str = None,
            folder: str = None
    ) -> Union[str, List[str]]:
        """
        Upload a file to Azure Blob Storage.

        Args:
            file: The file(s) to upload - can be bytes, BytesIO, MediaFile, or a file path string
            file_name: Optional name(s) for the file. If None, a UUID will be generated for each one.
            folder: The container name in Azure Blob Storage (required)

        Returns:
            str: The URL(s) of the uploaded blob(s)

        Raises:
            ValueError: If folder (container name) is not provided
        """
        if folder is None:
            raise ValueError("Folder aka container name must be provided for Azure Blob upload")

        if not isinstance(file, list):
            file = [file]

        file = [MediaFile().from_any(f) for f in file]

        # make sure file_name is a list
        if not isinstance(file_name, list):
            file_name = [file_name]

        # replace None with UUID
        file_name = [
            f if f is not None and isinstance(f, str) else str(uuid.uuid4())
            for f in file_name
        ]
        # make file_name list have the same length as file(s)
        # if too less entries fill with file_name_{i} if too many entries ignore additional ones
        if len(file_name) != len(file):
            file_name = [
                file_name[i] if i < len(file_name) else f"file_name_{i}"
                for i in range(len(file))
            ]

        jobs = []
        urls = []
        async with self._get_blob_service_client(async_mode=True) as bc:
            for f, fn in zip(file, file_name):
                blob_client = bc.get_blob_client(container=folder, blob=fn)
                b = f.to_bytes()
                up = blob_client.upload_blob(b, overwrite=True)
                jobs.append(up)
                urls.append(blob_client.url)

        await gather(*jobs)

        if len(urls) == 1:
            return urls[0]

        return urls

    def download(self, url: str, save_path: str = None, *args, **kwargs) -> Union[MediaFile, None, str]:
        parsed_url = urlparse(url)
        container_name = parsed_url.path.split('/')[1]
        blob_name = '/'.join(parsed_url.path.split('/')[2:])

        blob_client = self._get_blob_service_client(async_mode=False).get_blob_client(container=container_name, blob=blob_name)

        try:
            blob_data = blob_client.download_blob()
        except ResourceNotFoundError as e:
            print(f"An error occurred: {e}")
            return None

        if save_path is None:
            return MediaFile(file_name=url).from_bytes(blob_data.readall())

        with open(save_path, "wb") as f:
            blob_data.readinto(f)
        return save_path

    def _parse_and_validate_url(self, url: str) -> tuple[str, str]:
        """
        Parse and validate a blob URL, extracting container and blob names.

        Args:
            url: The URL of the blob to parse

        Returns:
            tuple: (container_name, blob_name)

        Raises:
            ValueError: If the URL doesn't belong to this storage provider
        """
        parsed_url = urlparse(url)
        container_name = parsed_url.path.split('/')[1]
        blob_name = '/'.join(parsed_url.path.split('/')[2:])

        service_client = self._get_blob_service_client(async_mode=False)
        if service_client.url not in url:
            raise ValueError("File does not belong to this storage provider.")

        return container_name, blob_name

    async def _delete_single_blob_async(self, url: str) -> bool:
        """
        Delete a single blob asynchronously.

        Args:
            url: The URL of the blob to delete

        Returns:
            bool: True if deletion was successful, False otherwise
        """
        try:
            container_name, blob_name = self._parse_and_validate_url(url)
            async with self._get_blob_service_client(async_mode=True) as service_client:
                blob_client = service_client.get_blob_client(
                    container=container_name,
                    blob=blob_name
                )
                await blob_client.delete_blob()
                return True
        except ResourceNotFoundError:
            print(f"The file {container_name}/{blob_name} was not found.")
            return False
        except Exception as e:
            print(f"An error occurred: {e}")
            return False

    def _delete_single_blob_sync(self, url: str) -> bool:
        """
        Delete a single blob synchronously.

        Args:
            url: The URL of the blob to delete

        Returns:
            bool: True if deletion was successful, False otherwise
        """
        try:
            container_name, blob_name = self._parse_and_validate_url(url)
            service_client = self._get_blob_service_client(async_mode=False)
            blob_client = service_client.get_blob_client(
                container=container_name,
                blob=blob_name
            )
            blob_client.delete_blob()
            return True
        except ResourceNotFoundError:
            print(f"The file {container_name}/{blob_name} was not found.")
            return False
        except Exception as e:
            print(f"An error occurred: {e}")
            return False

    def delete(self, url: Union[str, List[str]], *args, **kwargs) -> Union[bool, List[bool]]:
        """
        Delete a file or list of files from Azure Blob Storage synchronously.

        Args:
            url: Single URL or list of URLs to delete

        Returns:
            Union[bool, List[bool]]: Result(s) of deletion operation(s)
        """
        if not url:
            return False

        if isinstance(url, str):
            return self._delete_single_blob_sync(url)

        if isinstance(url, list):
            return [self._delete_single_blob_sync(u) for u in url]

        return False

    async def delete_async(self, url: Union[str, List[str]], *args, **kwargs) -> Union[bool, List[bool]]:
        """
        Delete a file or list of files from Azure Blob Storage asynchronously.
        Args:
            url: Single URL or list of URLs to delete
        Returns:
            Union[bool, List[bool]]: Result(s) of deletion operation(s)
        """
        if not url:
            return False

        if isinstance(url, str):
            return await self._delete_single_blob_async(url)

        if isinstance(url, list):
            tasks = [self._delete_single_blob_async(u) for u in url]
            return await gather(*tasks)

        return False

    def create_temporary_upload_link(
            self, time_limit: int = 20, container: str = "upload", blob_name: str = None, **kwargs
    ) -> Union[str, None]:
        """
        Generate a temporarily upload link (SAS URL) for a specific blob.
        :param container: Name of the Azure Blob Storage container.
        :param blob_name: Name of the blob (file) to be uploaded. If none generates random id.
        :param time_limit: Time in minutes for the SAS token to remain valid.
        :return: A SAS URL for direct upload.
        """
        if blob_name is None:
            blob_name = uuid.uuid4()

        try:
            bc = self._get_blob_service_client(async_mode=False)
            sas_token = generate_blob_sas(
                account_name=bc.account_name,
                container_name=container,
                blob_name=blob_name,
                account_key=bc.credential.account_key,
                permission=BlobSasPermissions(write=True),
                expiry=datetime.utcnow() + timedelta(minutes=time_limit)
            )

            blob_url = bc.get_blob_client(container=container, blob=blob_name).url
            return f"{blob_url}?{sas_token}"

        except Exception as e:
            print(f"An error occurred while generating SAS token: {e}")
            return None

    @requires("httpx")
    @staticmethod
    def upload_with_temporary_upload_link(sas_url: str, file: Union[bytes, io.BytesIO, MediaFile, str]) -> bool:
        """
        Upload a file directly to a given SAS URL.
        :param sas_url: The SAS URL for the blob.
        :param file: The file to upload.
        :return: True if the upload succeeds, False otherwise.
        """
        try:
            file = MediaFile().from_any(file)
            headers = {"x-ms-blob-type": "BlockBlob"}
            response = httpx.put(sas_url, content=file.to_bytes(), headers=headers)
            return response.status_code == 201
        except Exception as e:
            print(f"An error occurred during SAS upload: {e}")
            return False

    @requires("httpx")
    @staticmethod
    async def async_upload_with_temporary_upload_link(sas_url: str, file: Union[bytes, io.BytesIO, MediaFile, str]) -> bool:
        """
        Asynchronously upload a file directly to a given SAS URL.
        :param sas_url: The SAS URL for the blob.
        :param file: The file to upload.
        :return: True if the upload succeeds, False otherwise.
        """
        try:
            file = MediaFile().from_any(file)
            headers = {"x-ms-blob-type": "BlockBlob", "x-ms-if-none-match": "*"}
            async with httpx.AsyncClient() as client:
                response = await client.put(sas_url, content=file.to_bytes(), headers=headers)
                return response.status_code == 201
        except Exception as e:
            print(f"An error occurred during async SAS upload: {e}")
            return False


