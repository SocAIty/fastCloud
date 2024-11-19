from typing import Union

from fastCloud.core.i_cloud_storage import CloudStorage
from fastCloud.core.providers.azure_storage import AzureBlobStorage
from fastCloud.core.providers.s3_storage import S3Storage


def create_cloud_storage(
        # for azure
        azure_sas_access_token: str = None,
        azure_connection_string: str = None,
        # for s3
        s3_endpoint_url: str = None,
        s3_access_key_id: str = None,
        s3_access_key_secret: str = None
) -> Union[CloudStorage, None]:
    """
    Creates a cloud storage instance based on the configuration. If no configuration is given, None is returned.
    """
    if azure_sas_access_token or azure_connection_string:
        return AzureBlobStorage(sas_access_token=azure_sas_access_token, connection_string=azure_connection_string)

    if s3_endpoint_url or s3_access_key_id or s3_access_key_secret:
        return S3Storage(s3_endpoint_url, s3_access_key_id, s3_access_key_secret)

    return None
