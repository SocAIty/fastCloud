import io
from typing import Union, Optional

from fastCloud.core import FastCloud
from media_toolkit import MediaFile


class CloudStorage(FastCloud):
    """
    This is the interface for cloud storage services. Implement this interface to add a new cloud storage provider.
    """
    def upload(
            self,
            file: Union[bytes, io.BytesIO, MediaFile, str],
            file_name: str = None,
            folder: Optional[str] = None
    ) -> str:
        """
        Uploads a file to the cloud storage.
        :param file: The file data to upload. Is parsed to MediaFile if not already.
        :param file_name: The name of the file on the cloud storage. If None an uuid is generated.
        :param folder: Azure container-name or S3 bucket-name to upload the file to. If none default is used.
        :return: The URL of the uploaded file.
        """
        raise NotImplementedError("Implement in subclass")

    async def upload_async(
            self,
            file: Union[bytes, io.BytesIO, MediaFile, str],
            file_name: str = None,
            folder: Optional[str] = None
    ) -> str:
        """
        Uploads a file to the cloud storage asynchronously.
        :param file: The file data to upload. Is parsed to MediaFile if not already.
        :param file_name: The name of the file on the cloud storage. If None an uuid is generated.
        :param folder: Azure container-name or S3 bucket-name to upload the file to. If none default is used.
        :return: The URL of the uploaded file.
        """
        raise NotImplementedError("Implement in subclass")

    def download(self, url: str, save_path: str = None, *args, **kwargs) -> Union[MediaFile, None, str]:
        """
        Downloads a file from the cloud storage.
        :param url: The URL of the file to download.
        :param save_path: The path to save the downloaded file to. If None a BytesIO object is returned.
        """
        raise NotImplementedError("Implement in subclass")

    async def download_async(self, url: str, save_path: str = None, *args, **kwargs) -> Union[MediaFile, None, str]:
        """
        Downloads a file from the cloud storage asynchronously.
        :param url: The URL of the file to download.
        :param save_path: The path to save the downloaded file to. If None a BytesIO object is returned.
        """
        raise NotImplementedError("Implement in subclass")

    def delete(self, url: str, *args, **kwargs) -> bool:
        """
        Deletes a file from the cloud storage.
        :param url: The URL of the file to delete.
        :return: True if the file was deleted successfully
        """
        raise NotImplementedError("Implement in subclass")

    def create_temporary_upload_link(self, *args, time_limit: int = 20, **kwargs) -> str:
        """
        Creates a temporary link to upload a file to the cloud storage.
        :return: The URL to upload the file to.
        """
        raise NotImplementedError("Implement in subclass")
