from fastCloud.core.cloud_storage_factory import create_fast_cloud
from fastCloud.core import FastCloud, ReplicateUploadAPI, AzureBlobStorage, S3Storage, SocaityUploadAPI, CloudStorage

__all__ = [
    "create_fast_cloud",
    "FastCloud",
    "ReplicateUploadAPI",
    "AzureBlobStorage",
    "S3Storage",
    "SocaityUploadAPI",
    "CloudStorage"
]
