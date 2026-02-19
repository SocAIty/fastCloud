import io
import multiprocessing
import time
import uuid
import os
from typing import Optional, Union, List
from urllib.parse import urlparse

from media_toolkit import MediaFile, MediaList
from media_toolkit.utils.dependency_requirements import requires

from fastCloud.core.storage_providers.i_cloud_storage import CloudStorage

try:
    import boto3
    from boto3 import session
    from boto3.s3.transfer import TransferConfig
    from botocore.config import Config
except ImportError:
    pass

from tqdm import tqdm

class S3Storage(CloudStorage):
    @requires("boto3")
    def __init__(
            self,
            endpoint_url: str = None,
            access_key_id: str = None,
            access_key_secret: str = None,
    ):
        self.endpoint_url = endpoint_url
        self.access_key_id = access_key_id
        self.secret_access_key = access_key_secret

        self.transfer_config = TransferConfig(
            multipart_threshold=1024 * 25,
            max_concurrency=multiprocessing.cpu_count(),
            multipart_chunksize=1024 * 25,
            use_threads=True
        )

        self._boto_client = None

    def get_boto_client(self) -> 'boto3.client':
        if self._boto_client is not None:
            return self._boto_client

        if not all([self.endpoint_url, self.access_key_id, self.secret_access_key]):
            raise Exception("No or invalid bucket endpoint configuration")

        region = self.extract_region_from_url(self.endpoint_url)
        bucket_session = session.Session()
        boto_config = Config(
            signature_version='s3v4',
            retries={'max_attempts': 3, 'mode': 'standard'}
        )
        self._boto_client = bucket_session.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            config=boto_config,
            region_name=region
        )

        return self._boto_client

    def upload_in_memory_object(
            self,
            file_name: str,
            file_data: Union[bytes, io.BytesIO],
            bucket_name: Optional[str] = None,
            content_type: str = "application/octet-stream"
    ) -> str:
        boto_client = self.get_boto_client()

        if not bucket_name:
            bucket_name = time.strftime('%m-%y')

        if isinstance(file_data, io.BytesIO):
            file_data.seek(0)
        else:
            file_data = io.BytesIO(file_data)

        file_size = file_data.getbuffer().nbytes

        with tqdm(total=file_size, unit='B', unit_scale=True, desc=file_name) as progress_bar:
            boto_client.upload_fileobj(
                file_data,
                bucket_name,
                file_name,
                ExtraArgs={
                    'ContentType': content_type,
                    'ACL': 'public-read' # Change to 'private' if needed
                },
                Config=self.transfer_config,
                Callback=progress_bar.update
            )

        return boto_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': file_name},
            ExpiresIn=604800
        )

    def download_file(self, url: str, save_path: str = None) -> str:
        """
        Downloads a file by parsing the S3 URL to find the Bucket and Key.
        """
        boto_client = self.get_boto_client()
        
        # Parse URL to extract Bucket and Key
        parsed = urlparse(url)
        path_parts = parsed.path.lstrip('/').split('/')
        bucket = path_parts[0]
        key = '/'.join(path_parts[1:])
        
        # Remove query parameters (like signature) from key if present
        key = key.split('?')[0]

        if not save_path:
            save_path = os.path.join(os.getcwd(), os.path.basename(key))

        boto_client.download_file(bucket, key, save_path)
        return save_path

    def upload(
            self,
            file: Union[bytes, io.BytesIO, MediaFile, MediaList, str, list],
            file_name: Union[str, list] = None,
            folder: Optional[str] = None
    ) -> Union[str, List[str]]:
        # 1. Normalize input to list of MediaFiles
        if isinstance(file, MediaList):
            files_to_upload = file._media_files
        elif not isinstance(file, list):
            files_to_upload = [file]
        else:
            files_to_upload = file

        media_files = [
            f if isinstance(f, MediaFile) else MediaFile().from_any(f)
            for f in files_to_upload
        ]

        # 2. Normalize file_names
        if file_name is None:
            provided_names = []
        elif not isinstance(file_name, list):
            provided_names = [file_name]
        else:
            provided_names = file_name

        # 3. Resolve names with Extensions
        final_urls = []
        for i, mf in enumerate(media_files):
            # Check for name in: 1. Args, 2. MediaFile object, 3. UUID fallback
            if i < len(provided_names) and provided_names[i]:
                name = provided_names[i]
            elif mf.file_name and mf.file_name not in ["file", "media_file"]:
                name = mf.file_name
            else:
                ext = mf.extension
                name = f"{uuid.uuid4()}{'.' + ext if ext else ''}"

            # Ensure we have the right buffer
            data = io.BytesIO(mf.to_bytes())
            
            url = self.upload_in_memory_object(
                file_name=name, 
                file_data=data, 
                bucket_name=folder, 
                content_type=mf.content_type
            )
            final_urls.append(url)

        return final_urls[0] if len(final_urls) == 1 else final_urls

    @staticmethod
    def extract_region_from_url(endpoint_url):
        parsed_url = urlparse(endpoint_url)
        if '.s3.' in endpoint_url:
            return endpoint_url.split('.s3.')[1].split('.')[0]
        if parsed_url.netloc.endswith('.digitaloceanspaces.com'):
            return endpoint_url.split('.')[1].split('.digitaloceanspaces.com')[0]
        return 'us-east-1' # Default fallback