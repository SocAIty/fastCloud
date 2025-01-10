import io
from typing import Union

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

    def __init__(self, api_key: str, upload_endpoint="https://socaity.ai.api/v1/files", *args, **kwargs):
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

    def _process_upload_response(self, response: Response) -> str:
        """Process Socaity-specific response format.

        Args:
            response (Response): The HTTP response from Socaity.

        Returns:
            str: The temporary upload URL.

        Raises:
            Exception: If getting the temporary URL fails.
        """
        if response.status_code not in [200, 201]:
            raise Exception("Failed to get temporary upload URL")
        return response.json().get("upload_url")

    async def upload_async(self, file: Union[bytes, io.BytesIO, MediaFile, str], *args, **kwargs) -> str:
        """Upload a file using Socaity's two-step upload process.

        Args:
            file: The file to upload.

        Returns:
            str: The URL of the uploaded file.
        """
        async with self.http_client.get_async_client() as client:
            # Get temporary upload URL
            temp_url_response = await client.post(
                url=self.upload_endpoint,
                headers=self.get_auth_headers()
            )
            sas_url = self._process_upload_response(temp_url_response)

            # Upload to temporary URL
            await self._upload_to_temporary_url(client, sas_url, file)
            return sas_url

