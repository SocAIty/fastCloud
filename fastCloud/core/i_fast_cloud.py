from typing import Union, List, Any
from media_toolkit import IMediaContainer, IMediaFile, MediaFile, MediaDict, MediaList, media_from_any


class FastCloud:
    """
    This is the interface for cloud storage services. Implement this interface to add a new cloud storage provider.
    """
    def _upload_files(self, files: Union[MediaFile, List[MediaFile]], *args, **kwargs) -> Union[str, List[str]]:
        """
        Upload a file or a list of files to the cloud.
        :param files: The list of files to upload.
        :return: The URL(s) of the uploaded file(s).
        """
        raise NotImplementedError("Implement in subclass")

    async def _upload_files_async(self, files: Union[MediaFile, List[MediaFile]], *args, **kwargs) -> Union[str, List[str]]:
        """
        Upload a file or a list of files to the cloud asynchronously.
        :param files: The list of files to upload.
        :return: The URL(s) of the uploaded file(s).
        """
        raise NotImplementedError("Implement in subclass")

    def upload(self, file: Union[IMediaContainer, MediaFile, Any], *args, **kwargs) -> Union[str, List[str], dict]:
        """
        Upload one or more file(s) to the cloud.
        :param file: The file(s) to upload. The input is parsed to MediaFile if it is not already.
        :return:
            In case of input was a single file: The URL of the uploaded file.
            In case of input was list/MediaList of files: A list of URLs of the uploaded files.
            In case of input was dict/MediaDict of files: A dict with {key: url} pairs.
        """
        if isinstance(file, MediaDict):
            uploads = {}
            leaf_files = file.get_leaf_files()
            if len(leaf_files) > 0:
                uploaded_leafes = self._upload_files(list(leaf_files.values()), *args, **kwargs)
                if isinstance(uploaded_leafes, str):
                    uploaded_leafes = [uploaded_leafes]
                uploads.update(dict(zip(leaf_files.keys(), uploaded_leafes)))

            media_containers = file.get_media_containers()
            if len(media_containers) > 0:
                uploaded_nested = {key: self.upload(container, *args, **kwargs) for key, container in media_containers.items()}
                uploads.update(uploaded_nested)
            return uploads

        elif isinstance(file, MediaList):
            includes_containers = any([isinstance(f, IMediaContainer) for f in file])
            if includes_containers:
                # we upload sequentially
                return [self.upload(f, *args, **kwargs) for f in file]
            else:
                return self._upload_files(file.get_processable_files().to_list(), *args, **kwargs)

        elif isinstance(file, MediaFile):
            return self._upload_files(file, *args, **kwargs)

        file = media_from_any(file)
        return self.upload(file, *args, **kwargs)

    async def upload_async(
        self,
        file: Union[IMediaContainer, MediaFile, Any],
        *args, **kwargs
    ) -> Union[str, List[str], dict]:
        """
        Upload one or more file(s) to the cloud.
        :param file: The file(s) to upload. The input is parsed to MediaFile if it is not already.
        :return:
            In case of input was a single file: The URL of the uploaded file.
            In case of input was list/MediaList of files: A list of URLs of the uploaded files.
            In case of input was dict/MediaDict of files: A dict with {key: url} pairs.
        """
        if isinstance(file, MediaDict):
            uploads = {}
            leaf_files = file.get_leaf_files()
            if len(leaf_files) > 0:
                uploaded_leafes = await self._upload_files_async(list(leaf_files.values()), *args, **kwargs)
                if isinstance(uploaded_leafes, str):
                    uploaded_leafes = [uploaded_leafes]
                uploads.update(dict(zip(leaf_files.keys(), uploaded_leafes)))

            media_containers = file.get_media_containers()
            if len(media_containers) > 0:
                uploaded_nested = {key: await self.upload_async(container, *args, **kwargs) for key, container in media_containers.items()}
                uploads.update(uploaded_nested)
            return uploads

        elif isinstance(file, MediaList):
            includes_containers = any([isinstance(f, IMediaContainer) for f in file])
            if includes_containers:
                # we upload sequentially
                return [await self.upload_async(f, *args, **kwargs) for f in file]
            else:
                return await self._upload_files_async(file.get_processable_files(), *args, **kwargs)
        elif isinstance(file, MediaFile):
            return await self._upload_files_async(file, *args, **kwargs)

        file = media_from_any(file)
        return await self.upload_async(file, *args, **kwargs)

    def download(self, url: str, *args, **kwargs) -> IMediaFile:
        """
        Downloads a file from the cloud storage and parses it to MediaFile
        :param url: The URL of the file to download.
        :return: The downloaded file parsed to MediaFile.
        """
        raise NotImplementedError("Implement in subclass")

    async def download_async(self, url: str, *args, **kwargs) -> IMediaFile:
        """
        Downloads a file from the cloud storage and parses it to MediaFile
        :param url: The URL of the file to download.
        :return: The downloaded file parsed to MediaFile.
        """
        raise NotImplementedError("Implement in subclass")

    def delete(self, url: str, *args, **kwargs) -> bool:
        """
        Deletes a file from the cloud storage.
        :param url: The URL of the file to delete.
        :return: True if the file was deleted successfully
        """
        raise NotImplementedError("Implement in subclass")

    def create_temporary_upload_link(self, time_limit: int = 20, *args, **kwargs) -> str:
        """
        Creates a temporary link to upload a file to the cloud storage.
        :return: The URL to upload the file to.
        """
        raise NotImplementedError("Implement in subclass")
