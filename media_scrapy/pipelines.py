import os
import shutil
import logging
from os import path
from scrapy import Spider
from scrapy.http import Request
from itemadapter import ItemAdapter
from media_scrapy.items import MediaFiles
from typing import List
from typeguard import typechecked

logger = logging.getLogger(__name__)


@typechecked
class DropUnneededForDownloadMediaFilesPipeline:
    def process_item(self, item: MediaFiles, spider: Spider) -> MediaFiles:
        assert len(item["file_urls"]) == len(item["file_paths"])
        assert len(item["file_urls"]) == len(item["file_contents"])
        new_file_urls: List[str] = []
        new_file_paths: List[str] = []
        new_file_contents: List[None] = []
        for file_url, file_path, file_content in zip(
            item["file_urls"], item["file_paths"], item["file_contents"]
        ):
            if path.exists(file_path):
                continue

            if file_content is not None:
                os.makedirs(path.dirname(file_path), exist_ok=True)
                with open(file_path, "wb") as f:
                    f.write(file_content)
                logger.debug(
                    f"Save file content: {len(file_content)} bytes -> {file_path}"
                )
                continue

            assert isinstance(file_url, str)
            assert isinstance(file_path, str)
            assert file_content is None

            new_file_urls.append(file_url)
            new_file_paths.append(file_path)
            new_file_contents.append(file_content)

        item["file_urls"] = new_file_urls
        item["file_paths"] = new_file_paths
        item["file_contents"] = new_file_contents

        return item


@typechecked
class SaveDownloadedMediaFilesPipeline:
    def process_item(self, item: MediaFiles, spider: Spider) -> MediaFiles:
        download_dir = spider.settings.get("FILES_STORE")
        assert isinstance(download_dir, str)
        assert len(item["file_paths"]) == len(item["files"])
        for downloaded_file, save_path in zip(item["files"], item["file_paths"]):
            downloaded_file_path = path.join(download_dir, downloaded_file["path"])

            os.makedirs(path.dirname(save_path), exist_ok=True)

            shutil.move(downloaded_file_path, save_path)

            logger.debug(
                f"Downloaded file moved: {downloaded_file_path} -> {save_path}"
            )

        return item
