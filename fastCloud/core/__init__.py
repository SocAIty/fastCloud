from fastCloud.core.i_fast_cloud import FastCloud
from fastCloud.core.storage_providers.i_cloud_storage import CloudStorage
from fastCloud.core.api_providers.i_upload_api import BaseUploadAPI

from .storage_providers.azure_storage import AzureBlobStorage
from .storage_providers.s3_storage import S3Storage

from .cloud_storage_factory import create_fast_cloud

