import io
from typing import Union, List

from media_toolkit import MediaFile, MediaDict, MediaList


class FastCloud:
    """
    This is the interface for cloud storage services. Implement this interface to add a new cloud storage provider.
    """
    def upload(
            self, file: Union[MediaFile, MediaDict, MediaList,bytes, io.BytesIO, str, list, dict], *args, **kwargs
    ) -> Union[str, List[str], dict]:
        """
        Upload one or more file(s) to the cloud.
        :param file: The file(s) data to upload. Each file is parsed to MediaFile if it is not already.
        :return:
            In case of a single file: The URL of the uploaded file.
            In case of multiple files: A list of URLs of the uploaded files.
            In case of a dict: A dict with {key: url} pairs.
        """
        raise NotImplementedError("Implement in subclass")

    async def upload_async(
            self, file: Union[MediaFile, MediaDict, MediaList, bytes, io.BytesIO, MediaFile, str, list], *args, **kwargs
    ) -> Union[str, List[str]]:
        """
        Upload one or more file(s) to the cloud.
        :param file: The file(s) data to upload. Each file is parsed to MediaFile if it is not already.
        :return:
            In case of a single file: The URL of the uploaded file.
            In case of multiple files: A list of URLs of the uploaded files.
            In case of a dict: A dict with {key: url} pairs.
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

    def create_temporary_upload_link(self, time_limit: int = 20, *args, **kwargs) -> str:
        """
        Creates a temporary link to upload a file to the cloud storage.
        :return: The URL to upload the file to.
        """
        raise NotImplementedError("Implement in subclass")
