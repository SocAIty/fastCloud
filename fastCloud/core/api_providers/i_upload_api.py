import io
from abc import ABC, abstractmethod
from asyncio import gather
from typing import Union, Optional, List

from fastCloud.core.i_fast_cloud import FastCloud
from fastCloud.core.api_providers.HTTPClientManager import HTTPClientManager
from media_toolkit import MediaFile

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

    def upload(self, file: Union[bytes, io.BytesIO, MediaFile, str, list], *args, **kwargs) -> Union[str, List[str]]:
        """Upload a file synchronously.

        Args:
            file: The file(s) to upload.

        Returns:
            str: the URL or a list of URLs of the uploaded file(s).
        """
        if not isinstance(file, list):
            file = [file]

        file = [MediaFile().from_any(f) for f in file]

        with self.http_client.get_client() as client:
            for f in file:
                response = client.post(
                    url=self.upload_endpoint,
                    files={"content": f.to_httpx_send_able_tuple()},
                    headers=self.get_auth_headers(),
                    timeout=60
                )
            return self._process_upload_response(response)

    async def upload_async(self, file: Union[bytes, io.BytesIO, MediaFile, str, list], *args, **kwargs) -> Union[str, List[str]]:
        """Upload a file asynchronously.

        Args:
            file: The file(s) to upload.

        Returns:
            str: URL of the uploaded file.
        """
        if not isinstance(file, list):
            file = [file]

        file = [MediaFile().from_any(f) for f in file]

        async with self.http_client.get_async_client() as client:
            uploads = [
                client.post(
                    url=self.upload_endpoint,
                    files={"content": f.to_httpx_send_able_tuple()},
                    headers=self.get_auth_headers(),
                    timeout=60
                )
                for f in file
            ]
            responses = await gather(*uploads)

            return self._process_upload_response(responses)
