from typing import Union
from fastCloud.core import (
    CloudStorage, FastCloud, AzureBlobStorage, S3Storage, BaseUploadAPI, ReplicateBaseUploadAPI, SocaityBaseUploadAPI
)


def create_fast_cloud(
        # for azure
        azure_sas_access_token: str = None,
        azure_connection_string: str = None,
        # for s3
        s3_endpoint_url: str = None,
        s3_access_key_id: str = None,
        s3_access_key_secret: str = None,
        # for api_providers
        api_upload_endpoint: str = None,
        api_upload_api_key: str = None
) -> Union[FastCloud, CloudStorage, BaseUploadAPI, None]:
    """
    Creates a cloud storage instance based on the configuration. If no configuration is given, None is returned.
    """
    if azure_sas_access_token or azure_connection_string:
        return AzureBlobStorage(sas_access_token=azure_sas_access_token, connection_string=azure_connection_string)

    if s3_endpoint_url or s3_access_key_id or s3_access_key_secret:
        return S3Storage(s3_endpoint_url, s3_access_key_id, s3_access_key_secret)

    if api_upload_endpoint:
        if "socaity" in api_upload_endpoint:
            return SocaityBaseUploadAPI(api_upload_endpoint, api_upload_api_key)
        if "replicate" in api_upload_endpoint:
            return ReplicateBaseUploadAPI(api_upload_endpoint, api_upload_api_key)

    return None
