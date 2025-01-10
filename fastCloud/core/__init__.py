from .i_fast_cloud import FastCloud
from .storage_providers.i_cloud_storage import CloudStorage
from .api_providers import BaseUploadAPI, ReplicateUploadAPI, SocaityUploadAPI

from .storage_providers.azure_storage import AzureBlobStorage
from .storage_providers.s3_storage import S3Storage

from .cloud_storage_factory import create_fast_cloud

