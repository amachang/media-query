import os
import shutil
import logging
from os import path
from scrapy import Spider
from scrapy.http import Request
from itemadapter import ItemAdapter
from media_scrapy.items import MediaFiles

logger = logging.getLogger(__name__)


class DropDupeMediaFilesPipeline:
    def process_item(self, item: MediaFiles, spider: Spider) -> MediaFiles:
        assert len(item["file_urls"]) == len(item["file_paths"])
        new_file_urls = []
        new_file_paths = []
        for file_url, file_path in zip(item["file_urls"], item["file_paths"]):
            if not path.exists(file_path):
                new_file_urls.append(file_url)
                new_file_paths.append(file_path)

        item["file_urls"] = new_file_urls
        item["file_paths"] = new_file_paths

        return item


class SaveMediaFilesPipeline:
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
