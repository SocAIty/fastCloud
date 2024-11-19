
<h1 align="center" style="margin-top:-25px">FastCloud</h1>

<h3 align="center" style="margin-top:-10px">Up & Download files to any cloud storage.</h3>

Simplistic unified interface for uploading and downloading files to the cloud. 

Supports
- Azure Blob Storage  `:white_check_mark:`
- S3 Storages (Amazon, ...) `:question:`


# Installation

Install via pypi with:
```bash
# to support all cloud providers
pip install fastcloud[full]
# only support azure blob storage
pip install fastcloud[azure]
# only support s3
pip install fastcloud[s3]
```

Or check-out the repository and work from there.

# Usage

## Init storage  (Azure blob, S3 ...) 
To directly up and download files to the cloud storage provider, you can use the following code snippets.
```python 
from fastcloud import AzureBlobStorage, S3Storage, create_cloud_storage
# Create container of your choice
cloud_store = AzureBlobStorage(connection_string="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=...")
cloud_store = S3Storage(access_key_id, secret_access_key, region_name)
# Or use the factory to be more flexible
cloud_store = create_cloud_storage(... your credentials ...)
```
Recommendation: Use environment variables to store the cloud storage access tokens / credentials.

## Upload and download files

Option 1: Just with plain bytes io
```python 
# upload. Will create a file in the cloud with the name my_file
file_url = cloud_store.upload(file="path/to/file", file_name="my_file", folder="my_upload_dir")
# download. Will download the file and save it to the save-path
cloud_store.download(file_id, save_path="path/to/save")
```
Option 2: with media-toolkit. [Media-toolkit](https://github.com/SocAIty/media-toolkit) provides easy to use classes for images, videos, audio files.
```python 
from media_toolkit import ImageFile
# upload
my_img = ImageFile.from_np_array(my_cv2_img)
file_url = cloud_store.upload(file=my_img)
# download and parse as media_file
media_file = cloud_store.download(file_url)
```

# Tutorials

How to setup Azure Blob Storage and get connection string?
1. Go to portal.azure.com and login.
2. Create a storage account of your choice (choose Blob Storage option)
3. Navigate to storage account, click on containers and add container
4. Go back to your storage account. Click on Access keys and copy the connection string.
5. Add the connection string to your environment variables.


# Contribute

Missing a cloud provider?
- Just implement the missing class in core/providers with inheritance of the interface i_cloud_storage and make a pull request

Missing a feature? 
- Feel free to raise an issue. Better tough: just implement it and make a PR.

Also have a look at [media-toolkit](https://github.com/SocAIty/media-toolkit), [FastTaskAPI](https://github.com/SocAIty/FastTaskAPI) for your personal cloud solution.

### SUPPORT SOCAITY BY LEAVING A STAR