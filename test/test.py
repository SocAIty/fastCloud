import os
import asyncio

from fastCloud import FastCloud, AzureBlobStorage, SocaityUploadAPI, ReplicateUploadAPI
from media_toolkit import ImageFile, VideoFile, MediaList, MediaFile, MediaDict


TEST_DIR = os.path.dirname(__file__)
TEST_IMG = os.path.join(TEST_DIR, "test_img.png")
TEST_VIDEO = os.path.join(TEST_DIR, "test_video_ultra_short_short.mp4")


def _make_test_payloads():
    assert os.path.isfile(TEST_IMG), f"Missing test image at {TEST_IMG}"
    assert os.path.isfile(TEST_VIDEO), f"Missing test video at {TEST_VIDEO}"

    img = ImageFile().from_any(TEST_IMG)
    vid = VideoFile().from_any(TEST_VIDEO)
    media_list = MediaList([img, vid])
    media_dict = MediaDict({"image": img, "video": vid})
    nested_dict = MediaDict({
        "group": MediaList([img, vid]),
        "nested": MediaDict({"image": TEST_IMG}),
        "leaf": TEST_VIDEO,
    })
    nested_list = MediaList([
        img,
        MediaDict({"video": vid}),
        MediaList([img, vid])
    ])

    return {
        "single": img,
        "list": media_list,
        "dict": media_dict,
        "nested_dict": nested_dict,
        "nested_list": nested_list,
    }


def get_test_providers():
    """Get all available FastCloud providers with their configurations."""
    providers = []

    # Azure Blob Storage
    cs = os.environ.get("AZURE_BLOB_STORAGE_CONNECTION_STRING")
    sas = os.environ.get("AZURE_BLOB_STORAGE_SAS_URL")
 
    if cs or sas:
        azure = AzureBlobStorage(connection_string=cs, sas_access_token=sas)
        providers.append(azure)
    else:
        print("Skipping Azure: set AZURE_BLOB_STORAGE_CONNECTION_STRING or AZURE_BLOB_STORAGE_SAS_URL")

    # Replicate
    replicate_key = os.environ.get("REPLICATE_API_KEY")
    if replicate_key:
        replicate = ReplicateUploadAPI(api_key=replicate_key)
        providers.append(replicate)
    else:
        print("Skipping Replicate: set REPLICATE_API_KEY")

    # Socaity (async only)
    socaity_key = os.environ.get("SOCAITY_API_KEY")
    if socaity_key:
        socaity = SocaityUploadAPI(api_key=socaity_key)
        providers.append(socaity)
    else:
        print("Skipping Socaity: set SOCAITY_API_KEY")

    return providers


def _assert_urls_recursive(obj):
    if isinstance(obj, str):
        assert isinstance(obj, str) and obj.startswith("http"), f"Expected URL, got: {obj}"
    elif isinstance(obj, list):
        for x in obj:
            _assert_urls_recursive(x)
    elif isinstance(obj, dict):
        for v in obj.values():
            _assert_urls_recursive(v)
    else:
        raise AssertionError(f"Unexpected upload result type: {type(obj)}")


def do_upload(cloud: FastCloud, payload):
    if isinstance(cloud, AzureBlobStorage):
        return cloud.upload(payload, folder=os.getenv("AZURE_BLOB_STORAGE_CONTAINER", "upload"))
    else:
        return cloud.upload(payload)


async def do_upload_async(cloud: FastCloud, payload):
    if isinstance(cloud, AzureBlobStorage):
        return await cloud.upload_async(payload, folder=os.getenv("AZURE_BLOB_STORAGE_CONTAINER", "upload"))
    else:
        return await cloud.upload_async(payload)


def upload_and_assert(cloud: FastCloud, payload, test_name: str):
    urls = do_upload(cloud, payload)
    _assert_urls_recursive(urls)
    print(f"Upload {test_name} result:", urls)
    return urls


async def upload_and_assert_async(cloud: FastCloud, payload, test_name: str):
    urls = await do_upload_async(cloud, payload)
    _assert_urls_recursive(urls)
    print(f"Upload {test_name} result:", urls)
    return urls


def _upload_test(cloud: FastCloud, payloads: dict):
    print(f"Testing {cloud.__class__.__name__} upload (sync)")
    results = {
        test_name: upload_and_assert(cloud, payload, test_name)
        for test_name, payload in payloads.items()
    }
    return results


async def _upload_test_async(cloud: FastCloud, payloads: dict):
    print(f"Testing {cloud.__class__.__name__} upload (async)")
    results = {
        test_name: await upload_and_assert_async(cloud, payload, test_name)
        for test_name, payload in payloads.items()
    }
    return results


def _flatten_urls(obj):
    acc = []
    if isinstance(obj, str):
        acc.append(obj)
    elif isinstance(obj, list):
        for x in obj:
            acc.extend(_flatten_urls(x))
    elif isinstance(obj, dict):
        for v in obj.values():
            acc.extend(_flatten_urls(v))
    return acc


def _test_download(cloud: FastCloud, urls: list):
    print(f"Testing {cloud.__class__.__name__} download (sync)")
    urls = _flatten_urls(urls)
    for url in urls:
        downloaded = cloud.download(url)
        assert isinstance(downloaded, MediaFile)
        print(f"Download test result: {downloaded}")
    return downloaded


async def _test_download_async(cloud: FastCloud, urls: list):
    print(f"Testing {cloud.__class__.__name__} download (async)")
    urls = _flatten_urls(urls)
    for url in urls:
        downloaded = await cloud.download_async(url)
        assert isinstance(downloaded, MediaFile)
        print(f"Download test result: {downloaded}")
    return downloaded


def _test_delete(cloud: FastCloud, urls: list):
    print(f"Testing {cloud.__class__.__name__} delete (sync)")
    urls = _flatten_urls(urls)
    try:
        delete_result = cloud.delete(urls)
    except NotImplementedError:
        print(f"Delete not implemented for {cloud.__class__.__name__}")
        return True
    except Exception as e:
        print(f"Delete test failed: {e}")
        return False
    assert isinstance(delete_result, list) and all(isinstance(b, bool) for b in delete_result)
    print(f"Delete test result: {delete_result}")
    return delete_result


def test_all(cloud: FastCloud):
    payloads = _make_test_payloads()
    upload_results = _upload_test(cloud, payloads)
    _test_download(cloud, upload_results)
    delete_results = _test_delete(cloud, upload_results)
    return upload_results, delete_results


async def test_all_async(cloud: FastCloud):
    payloads = _make_test_payloads()
    upload_results = await _upload_test_async(cloud, payloads)
    delete_results = _test_delete(cloud, upload_results)
    return upload_results, delete_results


if __name__ == "__main__":
    providers = get_test_providers()
    print("RUNNING ALL TESTS (sync)")
    for cloud in providers:
        test_all(cloud)

    print("RUNNING ALL TESTS (async)")
    for cloud in providers:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(test_all_async(cloud))
