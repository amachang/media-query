from media_scrapy.pipelines import *
from media_scrapy.items import DownloadUrlItem, SaveFileContentItem
from scrapy.exceptions import DropItem
from scrapy import Item
from typing import Any
from pathlib import Path
from .utils import fake_spider
import pytest


def make_saved_content_item(file_path: Path, content: bytes) -> SaveFileContentItem:
    file_path.write_bytes(content)
    return SaveFileContentItem(file_path=str(file_path), file_content=content)


def make_saved_download_item(file_path: Path, url: str) -> DownloadUrlItem:
    file_path.touch()
    return DownloadUrlItem(
        url=url,
        file_path=str(file_path),
    )


def test_whole_pipelines(tmpdir: Any) -> None:
    tmpdir = Path(tmpdir)

    # prepare spider
    spider = fake_spider(tmpdir, {"FILES_STORE": str(tmpdir)})

    # save file path
    savedir = tmpdir.joinpath("save")
    savedir.mkdir(parents=True, exist_ok=True)
    foo_path = savedir.joinpath("foo.txt")
    bar_path = savedir.joinpath("bar.txt")
    baz_path = savedir.joinpath("baz.txt")
    baa_path = savedir.joinpath("baa.txt")

    # item creation
    other_item = Item()
    saved_content_item = make_saved_content_item(foo_path, b"foo")
    unsaved_content_item = SaveFileContentItem(
        file_path=str(bar_path), file_content=b"bar"
    )
    saved_download_item = make_saved_download_item(baz_path, "http://example.com/baz")
    unsaved_download_item = DownloadUrlItem(
        file_path=str(baa_path), url="http://example.com/baa"
    )

    # drop item pipeline
    drop_pipeline = DropUnneededItemPipeline()
    assert drop_pipeline.process_item(other_item, spider) == other_item
    with pytest.raises(DropItem):
        drop_pipeline.process_item(saved_content_item, spider)
    assert (
        drop_pipeline.process_item(unsaved_content_item, spider) == unsaved_content_item
    )
    with pytest.raises(DropItem):
        drop_pipeline.process_item(saved_download_item, spider)
    assert (
        drop_pipeline.process_item(unsaved_download_item, spider)
        == unsaved_download_item
    )

    # save known file content pipeline
    save_pipeline = SaveFileContentPipeline()
    assert save_pipeline.process_item(other_item, spider) == other_item

    assert not path.exists(unsaved_content_item["file_path"])
    assert (
        save_pipeline.process_item(unsaved_content_item, spider) == unsaved_content_item
    )
    assert path.exists(unsaved_content_item["file_path"])
    assert Path(unsaved_content_item["file_path"]).read_bytes() == b"bar"

    assert not path.exists(unsaved_download_item["file_path"])
    assert (
        save_pipeline.process_item(unsaved_download_item, spider)
        == unsaved_download_item
    )
    assert not path.exists(unsaved_download_item["file_path"])

    # prepare item for FilesPipelines
    prepare_pipeline = PrepareItemForFilesPipelines()
    assert prepare_pipeline.process_item(other_item, spider) == other_item
    assert (
        prepare_pipeline.process_item(unsaved_content_item, spider)
        == unsaved_content_item
    )

    files_item = prepare_pipeline.process_item(unsaved_download_item, spider)
    assert files_item["original_item"] == unsaved_download_item
    assert files_item["file_urls"] == [unsaved_download_item["url"]]

    # emulate download
    files_item["files"] = [{"path": "downloaded.txt", "status": "downloaded"}]
    tmp_downloaded_path = tmpdir.joinpath("downloaded.txt")
    tmp_downloaded_path.write_bytes(b"baa")
    assert tmp_downloaded_path.exists()
    print(tmp_downloaded_path)

    # SaveDownloadedFilePipeline
    downloaded_pipeline = SaveDownloadedFilePipeline()
    assert downloaded_pipeline.process_item(other_item, spider) == other_item
    assert (
        downloaded_pipeline.process_item(unsaved_content_item, spider)
        == unsaved_content_item
    )
    assert downloaded_pipeline.process_item(files_item, spider) == unsaved_download_item
    assert path.exists(unsaved_download_item["file_path"])
    assert Path(unsaved_download_item["file_path"]).read_bytes() == b"baa"
