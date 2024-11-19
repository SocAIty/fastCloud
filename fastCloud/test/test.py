import os

from fastCloud import AzureBlobStorage
from media_toolkit import ImageFile

test_img = "test_img.png"

def test_azure_blob_storage():
    # test uploads
    cs = os.environ.get("AZURE_BLOB_STORAGE_CONNECTION_STRING", None)
    container = AzureBlobStorage(connection_string=cs)
    #container = AzureBlobStorage(connection_string=cs)
    file_url = container.upload(test_img, file_name="test_img_from_file.png", folder="backend-model_description-meta")
    # test upload with media toolkit
    img = ImageFile().from_file(test_img)
    container.upload(img, "test_img_from_mt.png", "fastcloud_upload")

    # test download
    container.download(file_url, "test_img_from_file_download.png")


def test_s3_upload():
    a = 1



if __name__ == "__main__":
    test_azure_blob_storage()

    test_s3_upload()