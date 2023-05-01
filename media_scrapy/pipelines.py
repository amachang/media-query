import os
import shutil
import logging
from os import path
from scrapy import Spider, Item, Field
from scrapy.http import Request
from scrapy.exceptions import DropItem
from scrapy.pipelines.files import FilesPipeline
from itemadapter import ItemAdapter
from media_scrapy.items import DownloadUrlItem, SaveFileContentItem
from typing import List, Any, Tuple
from typeguard import typechecked

logger = logging.getLogger(__name__)


@typechecked
class ScrapyFilesPipelineItem(Item):
    file_urls = Field()
    files = Field()
    original_item = Field()


@typechecked
class DropUnneededItemPipeline:
    def process_item(self, item: Item, spider: Spider) -> Item:
        if isinstance(item, DownloadUrlItem) or isinstance(item, SaveFileContentItem):
            file_path = item["file_path"]
            if path.exists(file_path):
                raise DropItem(f"Already downloaded: {item}")
        return item


@typechecked
class SaveFileContentPipeline:
    def process_item(self, item: Item, spider: Spider) -> Item:
        if isinstance(item, SaveFileContentItem):
            file_path = item["file_path"]
            file_content = item["file_content"]
            os.makedirs(path.dirname(file_path), exist_ok=True)
            with open(file_path, "wb") as f:
                f.write(file_content)
                logger.debug(
                    f"Save file content: {len(file_content)} bytes -> {file_path}"
                )

        return item


@typechecked
class PrepareItemForFilesPipelines:
    def process_item(self, item: Item, spider: Spider) -> Item:
        if isinstance(item, DownloadUrlItem):
            url = item["url"]
            return ScrapyFilesPipelineItem(
                file_urls=[url],
                original_item=item,
            )
        else:
            return item


@typechecked
class SaveDownloadedFilePipeline:
    def process_item(self, item: Item, spider: Spider) -> Item:
        if isinstance(item, ScrapyFilesPipelineItem):
            download_dir = spider.settings.get("FILES_STORE")
            file_info_list = item["files"]
            assert len(file_info_list) == 1
            file_info = file_info_list[0]
            if file_info["status"] == "downloaded":
                original_item = item["original_item"]
                assert isinstance(original_item, DownloadUrlItem)
                save_file_path = original_item["file_path"]

                downloaded_file_path = path.join(download_dir, file_info["path"])
                assert path.exists(downloaded_file_path)
                save_dir = path.dirname(save_file_path)
                os.makedirs(save_dir, exist_ok=True)
                assert path.isdir(save_dir)
                shutil.move(downloaded_file_path, save_file_path)

                logger.debug(
                    f"Downloaded file moved: {downloaded_file_path} -> {save_file_path}"
                )
            return original_item
        else:
            return item
