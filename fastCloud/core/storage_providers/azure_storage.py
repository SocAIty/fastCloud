import uuid
from datetime import datetime, timedelta
from typing import Union
import io
from urllib.parse import urlparse

from fastCloud.core.storage_providers.i_cloud_storage import CloudStorage

try:
    from azure.core.exceptions import ResourceNotFoundError
    from azure.storage.blob import BlobServiceClient
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

        if sas_access_token:
            self.blob_service_client = BlobServiceClient(account_url=sas_access_token)
        elif connection_string:
            self.blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    def upload(
            self,
            file: Union[bytes, io.BytesIO, MediaFile, str],
            file_name: str = None,
            folder: str = None
    ) -> str:

        if folder is None:
            raise ValueError("Folder aka container name must be provided for Azure Blob upload")

        if file_name is None:
            file_name = uuid.uuid4()

        file = MediaFile().from_any(file)

        blob_client = self.blob_service_client.get_blob_client(container=folder, blob=file_name)

        b = file.to_bytes()
        blob_client.upload_blob(b, overwrite=True)
        return blob_client.url


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
        """
        Delete a file from the Azure Blob Storage.
        :param url: the url of the file to delete
        :return: true if the file was deleted, false otherwise
        """
        try:
            parsed_url = urlparse(url)
            container_name = parsed_url.path.split('/')[1]
            blob_name = '/'.join(parsed_url.path.split('/')[2:])
            blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)

            blob_client.delete_blob()
            return True
        except ResourceNotFoundError as e:
            print(f"The file {container_name}/{blob_name} was not found.")
            return False
        except Exception as e:
            print(f"An error occurred: {e}")
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
            sas_token = generate_blob_sas(
                account_name=self.blob_service_client.account_name,
                container_name=container,
                blob_name=blob_name,
                account_key=self.blob_service_client.credential.account_key,
                permission=BlobSasPermissions(write=True),
                expiry=datetime.utcnow() + timedelta(minutes=time_limit)
            )

            blob_url = self.blob_service_client.get_blob_client(container=container, blob=blob_name).url
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


