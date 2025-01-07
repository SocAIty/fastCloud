from fastCloud import BaseUploadAPI
from media_toolkit.utils.dependency_requirements import requires

try:
    import httpx
    from httpx import Response
except:
    pass

@requires("httpx")
class ReplicateUploadAPI(BaseUploadAPI):
    """Replicate-specific implementation of the upload API.

    Args:
        upload_endpoint (str): The Replicate upload endpoint.
        api_key (str): Replicate API key.
    """

    def __init__(self, api_key: str, upload_endpoint: str = "https://api.replicate.com/v1/files"):
        super().__init__(upload_endpoint, api_key)

    def _process_upload_response(self, response: Response) -> str:
        """Process Replicate-specific response format.

        Args:
            response (Response): The HTTP response from Replicate.

        Returns:
            str: The file URL.

        Raises:
            Exception: If the upload fails or URL extraction fails.
        """
        if response.status_code != 200:
            raise Exception(f"Failed to upload to Replicate. {response.text}")

        data = response.json()
        file_url = data.get("urls", {}).get("get")
        if not file_url:
            raise Exception(f"Failed to get file URL from Replicate response. {data}")
        return file_url
