import io
from abc import ABC, abstractmethod
from asyncio import gather
from typing import Union, Optional, List

from fastCloud.core.i_fast_cloud import FastCloud
from fastCloud.core.api_providers.HTTPClientManager import HTTPClientManager
from media_toolkit import MediaFile, MediaList, MediaDict

try:
    import httpx
    from httpx import AsyncClient, Client, Response
except:
    pass



class BaseUploadAPI(FastCloud, ABC):
    """Base class for upload API implementations using Template Method pattern.

    Args:
        upload_endpoint (str): The endpoint URL for uploads.
        api_key (str): Authentication API key.
    """

    def __init__(self, api_key: str, upload_endpoint: str = None, *args, **kwargs):
        self.upload_endpoint = upload_endpoint
        self.api_key = api_key
        self.http_client = HTTPClientManager()

    def get_auth_headers(self) -> dict:
        """Get authentication headers.

        Returns:
            dict: Headers dictionary with authentication.
        """
        return {"Authorization": f"Bearer {self.api_key}"}

    @abstractmethod
    def _process_upload_response(self, response: Union[Response, List[Response]]) -> Union[str, List[str]]:
        """Process the upload response to extract the file URL.

        Args:
            response (Response): The HTTP response to process.

        Returns:
            str: The URL of the uploaded file.

        Raises:
            Exception: If the response processing fails.
        """
        pass

    def download(self, url: str, save_path: Optional[str] = None, *args, **kwargs) -> Union[MediaFile, str]:
        """Download a file from the given URL.

        Args:
            url (str): URL to download from.
            save_path (Optional[str]): Path to save the file to.

        Returns:
            Union[MediaFile, str]: MediaFile object or save path if specified.
        """
        file = MediaFile(file_name=url).from_url(url, headers=self.get_auth_headers())
        if save_path is None:
            return file

        file.save(save_path)
        return save_path

    def upload(
            self, file: Union[MediaFile, MediaDict, MediaList, bytes, io.BytesIO, str, list, dict], *args, **kwargs
    ) -> Union[str, List[str], dict]:
        """
        Upload one or more file(s) to the cloud.
        :param file: The file(s) data to upload. Each file is parsed to MediaFile if it is not already.
        :return:
            In case of a single file: The URL of the uploaded file.
            In case of multiple files: A list of URLs of the uploaded files.
            In case of a dict: A dict with {key: url} pairs.
        """
        upload_data = MediaDict(download_files=False).from_any(file)
        uploaded_files = {}
        
        with self.http_client.get_client() as client:
            for k, f in upload_data.items():
                if isinstance(f, (MediaDict, dict)):
                    # Recursively handle nested dictionaries
                    uploaded_files[k] = self.upload(f, *args, **kwargs)
                elif isinstance(f, (MediaList, list)):
                    # Handle lists by uploading each item
                    uploaded_files[k] = self.upload(f, *args, **kwargs)
                else:
                    # Handle single file
                    response = client.post(
                        url=self.upload_endpoint,
                        files={"content": f.to_httpx_send_able_tuple()},
                        headers=self.get_auth_headers(),
                        timeout=60)
                    uploaded_files[k] = self._process_upload_response(response)

        # If input was a single file, return just the URL
        if len(uploaded_files) == 1 and not isinstance(file, (MediaDict, dict, MediaList, list)):
            return list(uploaded_files.values())[0]
        return uploaded_files

    async def upload_async(
            self, file: Union[MediaFile, MediaDict, MediaList, bytes, io.BytesIO, str, list, dict], *args, **kwargs
    ) -> Union[str, List[str], dict]:
        """
        Upload one or more file(s) to the cloud.
        :param file: The file(s) data to upload. Each file is parsed to MediaFile if it is not already.
        :return:
            In case of a single file: The URL of the uploaded file.
            In case of multiple files: A list of URLs of the uploaded files.
            In case of a dict: A dict with {key: url} pairs.
        """
        upload_data = MediaDict(download_files=False).from_any(file)
        uploaded_files = {}

        async with self.http_client.get_async_client() as client:
            for k, f in upload_data.items():
                if isinstance(f, (MediaDict, dict)):
                    # Recursively handle nested dictionaries
                    uploaded_files[k] = await self.upload_async(f, *args, **kwargs)
                elif isinstance(f, (MediaList, list)):
                    # Handle lists by uploading each item
                    uploaded_files[k] = await self.upload_async(f, *args, **kwargs)
                else:
                    # Handle single file
                    response = await client.post(
                        url=self.upload_endpoint,
                        files={"content": f.to_httpx_send_able_tuple()},
                        headers=self.get_auth_headers(),
                        timeout=60)
                    uploaded_files[k] = self._process_upload_response(response)

        # If input was a single file, return just the URL
        if len(uploaded_files) == 1 and not isinstance(file, (MediaDict, dict, MediaList, list)):
            return list(uploaded_files.values())[0]
        return uploaded_files
