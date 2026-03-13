from abc import ABC, abstractmethod
from typing import Union, Optional, List
import asyncio

from fastCloud.core.i_fast_cloud import FastCloud
from fastCloud.core.api_providers.HTTPClientManager import HTTPClientManager
from media_toolkit import MediaFile, media_from_any

try:
    from httpx import Response
except Exception:
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
    def _process_upload_response(self, response: Response) -> str:
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
        file = media_from_any(url, headers=self.get_auth_headers())
        if save_path is None:
            return file

        file.save(save_path)
        return save_path

    def _upload_files(self, files: Union[MediaFile, List[MediaFile]], *args, **kwargs) -> Union[str, List[str]]:
        """
        Upload a list of files to the cloud.
        :param files: The file or list of file to upload.
        :return: The URL(s) of the uploaded file(s).
        """
        if not isinstance(files, (MediaFile, list)):
            raise ValueError("files must be a MediaFile or list of MediaFile")

        if not isinstance(files, list):
            files = [files]

        with self.http_client.get_client() as client:
            # Handle single file
            uploaded_files = []
            for file in files:
                response = client.post(
                    url=self.upload_endpoint,
                    files={"content": file.to_httpx_send_able_tuple()},
                    headers=self.get_auth_headers(),
                    timeout=60
                )
                processed_response = self._process_upload_response(response)
                uploaded_files.append(processed_response)

        return uploaded_files if len(uploaded_files) > 1 else uploaded_files[0]

    async def _upload_files_async(self, files: Union[MediaFile, List[MediaFile]], *args, **kwargs) -> Union[str, List[str]]:
        """
        Upload a list of files to the cloud asynchronously.
        :param files: The list of files to upload.
        :return: The URL of the uploaded file.
        """
        if not isinstance(files, (MediaFile, list)):
            raise ValueError("files must be a MediaFile or list of MediaFile")

        if not isinstance(files, list):
            files = [files]

        async with self.http_client.get_async_client() as client:
            async_requests = [
                client.post(
                    url=self.upload_endpoint,
                    files={"content": file.to_httpx_send_able_tuple()},
                    headers=self.get_auth_headers(),
                    timeout=60
                ) for file in files
            ]
            responses = await asyncio.gather(*async_requests)

        uploaded_files = [self._process_upload_response(response) for response in responses]
        return uploaded_files if len(uploaded_files) > 1 else uploaded_files[0]
