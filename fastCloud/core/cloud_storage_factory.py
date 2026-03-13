from typing import Union
from fastCloud.core import (
    FastCloud, AzureBlobStorage, BaseUploadAPI, ReplicateUploadAPI, SocaityUploadAPI
)


def create_fast_cloud(
        # for azure
        azure_sas_access_token: str = None,
        azure_connection_string: str = None,
        # for api_providers
        api_upload_endpoint: str = None,
        api_upload_api_key: str = None
) -> Union[FastCloud, BaseUploadAPI, None]:
    """
    Creates a cloud storage instance based on the configuration. If no configuration is given, None is returned.
    """
    if azure_sas_access_token or azure_connection_string:
        return AzureBlobStorage(sas_access_token=azure_sas_access_token, connection_string=azure_connection_string)

    if api_upload_endpoint:
        if "socaity" in api_upload_endpoint:
            return SocaityUploadAPI(api_key=api_upload_api_key, upload_endpoint=api_upload_endpoint)
        if "replicate" in api_upload_endpoint:
            return ReplicateUploadAPI(api_key=api_upload_api_key, upload_endpoint=api_upload_endpoint)

    return None
