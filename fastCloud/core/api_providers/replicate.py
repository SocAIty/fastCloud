from typing import Union, List

from fastCloud.core.api_providers.i_upload_api import BaseUploadAPI
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
    def __init__(self, api_key: str, upload_endpoint: str = "https://api.replicate.com/v1/files", *args, **kwargs):
        super().__init__(api_key=api_key, upload_endpoint=upload_endpoint, *args, **kwargs)

    def _process_upload_response(self, response: Union[Response, List[Response]]) -> List[str]:
        """Process Replicate-specific response format.

        Args:
            response (Response): The HTTP response from Replicate.

        Returns:
            str: The file URL.

        Raises:
            Exception: If the upload fails or URL extraction fails.
        """
        if not isinstance(response, list):
            response = [response]

        urls = []
        for r in response:
            if r.status_code not in [200, 201]:
                raise Exception(f"Failed to upload to Replicate. {r.text}")

            data = r.json()
            file_url = data.get("urls", {}).get("get")
            if not file_url:
                raise Exception(f"Failed to get file URL from Replicate response. {data}")
            urls.append(file_url)

        return urls
