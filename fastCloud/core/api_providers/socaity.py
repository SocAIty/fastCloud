import asyncio
import io
from typing import Union, List

from fastCloud.core.api_providers.i_upload_api import BaseUploadAPI
from media_toolkit import MediaFile
from media_toolkit.utils.dependency_requirements import requires

try:
    import httpx
    from httpx import Response, AsyncClient
except:
    pass

@requires("httpx")
class SocaityUploadAPI(BaseUploadAPI):
    """Socaity-specific implementation of the upload API.

    Args:
        api_key (str): Socaity API key.
    """

    def __init__(self, api_key: str, upload_endpoint="https://api.socaity.ai/v0/files", *args, **kwargs):
        super().__init__(api_key=api_key, upload_endpoint=upload_endpoint, *args, **kwargs)

    async def _upload_to_temporary_url(self, client: AsyncClient, sas_url: str, file: MediaFile) -> None:
        """Upload a file to a temporary URL.

        Args:
            client (AsyncClient): The HTTP client to use.
            sas_url (str): The temporary upload URL.
            file (MediaFile): The file to upload.

        Raises:
            Exception: If the upload fails.
        """
        headers = {
            "x-ms-blob-type": "BlockBlob",
            "x-ms-if-none-match": "*"
        }

        response = await client.put(
            sas_url,
            content=file.to_bytes(),
            headers=headers
        )

        if response.status_code != 201:
            raise Exception(f"Failed to upload to temporary URL {sas_url}. Response: {response.text}")

    def _process_upload_response(self, response: Union[Response, List[Response]]) -> List[str]:
        """Process Socaity-specific response format.

        Args:
            response (Response): The HTTP response from Socaity.

        Returns:
            List[str]: A list of the temporary upload URL.

        Raises:
            Exception: If getting the temporary URL fails.
        """
        if not isinstance(response, list):
            response = [response]

        urls = []
        for resp in response:
            if resp.status_code not in [200, 201]:
                raise Exception("Failed to get temporary upload URL")
            urls.extend(resp.json())

        return urls

    async def upload_async(self, file: Union[bytes, io.BytesIO, MediaFile, str, list], *args, **kwargs) \
            -> Union[str, list[str]]:
        """Upload one ore more files using Socaity's two-step upload process.
        Args:
            file: The file or files to upload.

        Returns:
            str: The URL of the uploaded file. If multiple files are uploaded, a list of URLs is returned.
        """

        if not isinstance(file, list):
            file = [file]

        n_files = len(file)
        exts = [f.extension for f in file if isinstance(f, MediaFile)]
        exts = [ext for ext in exts if ext is not None]
        if len(exts) == 0:
            exts = None

        async with self.http_client.get_async_client() as client:
            # Get temporary upload URL
            temp_url_response = await client.post(
                url=self.upload_endpoint,
                json={"n_files": n_files, "file_extensions": exts},
                headers=self.get_auth_headers()
            )

            sas_urls = self._process_upload_response(temp_url_response)
            if not isinstance(sas_urls, list):
                sas_urls = [sas_urls]

            uploads = []
            for i, sas_url in enumerate(sas_urls):
                uploads.append(self._upload_to_temporary_url(client, sas_url, file[i]))

            await asyncio.gather(*uploads)

            if len(sas_urls) == 1:
                return sas_urls[0]

            return sas_urls


