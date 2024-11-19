import uuid
from typing import Union
import io
from urllib.parse import urlparse

from fastCloud.core.i_cloud_storage import CloudStorage

try:
    from azure.core.exceptions import ResourceNotFoundError
    from azure.storage.blob import BlobServiceClient
except ImportError:
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

        if sas_access_token:
            self.blob_service_client = BlobServiceClient(account_url=sas_access_token)
        elif connection_string:
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    def upload(
            self,
            file: Union[bytes, io.BytesIO, MediaFile, str],
            file_name: str = None,
            folder: str = None
    ) -> Union[str, None]:

        if folder is None:
            raise ValueError("Folder aka container name must be provided for Azure Blob upload")

        if file_name is None:
            file_name = uuid.uuid4()

        file = MediaFile().from_any(file)

        blob_client = self.blob_service_client.get_blob_client(container=folder, blob=file_name)

        try:
            b = file.to_bytes()
            blob_client.upload_blob(b, overwrite=True)
            return blob_client.url
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def download(self, url: str, save_path: str = None) -> Union[MediaFile, None, str]:
        parsed_url = urlparse(url)
        container_name = parsed_url.path.split('/')[1]
        blob_name = '/'.join(parsed_url.path.split('/')[2:])

        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)

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

    def delete(self, url: str) -> bool:
        parsed_url = urlparse(url)
        container_name = parsed_url.path.split('/')[1]
        blob_name = '/'.join(parsed_url.path.split('/')[2:])

        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        try:
            blob_client.delete_blob()
            return True
        except ResourceNotFoundError as e:
            print(f"An error occurred: {e}")
            return False
