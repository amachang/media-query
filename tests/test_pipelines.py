from media_scrapy.pipelines import *
from media_scrapy.items import MediaFiles
from typing import Any
from pathlib import Path
from .utils import fake_spider


def test_drop_dupe_media_files_piplines(tmpdir: Any) -> None:
    tmpdir = Path(tmpdir)
    pipeline = DropDupeMediaFilesPipeline()

    item = MediaFiles(
        file_urls=["http://example.com/aaa.txt", "http://example.com/bbb.txt"],
        file_paths=[str(tmpdir.joinpath("aaa.txt")), str(tmpdir.joinpath("bbb.txt"))],
    )

    tmpdir.joinpath("aaa.txt").touch()

    spider = fake_spider(tmpdir)

    item = pipeline.process_item(item, spider)
    assert len(item["file_urls"]) == 1
    assert item["file_urls"][0] == "http://example.com/bbb.txt"
    assert len(item["file_paths"]) == 1
    assert item["file_paths"][0] == str(tmpdir.joinpath("bbb.txt"))


def test_save_media_files_pipeline(tmpdir: Any) -> None:
    tmpdir = Path(tmpdir)
    pipeline = SaveMediaFilesPipeline()

    spider = fake_spider(tmpdir)

    srcdir = tmpdir.joinpath("src")
    dstdir = tmpdir.joinpath("dst")

    srcdir.mkdir()
    srcdir.joinpath("aaa.txt").write_text("foo bar baz")

    assert not dstdir.joinpath("bbb.txt").exists()

    item = MediaFiles(
        file_urls=["http://example.com/bbb.txt"],
        file_paths=[str(dstdir.joinpath("bbb.txt"))],
        files=[{"path": str(srcdir.joinpath("aaa.txt"))}],
    )

    item = pipeline.process_item(item, spider)

    assert dstdir.joinpath("bbb.txt").exists()
    assert dstdir.joinpath("bbb.txt").read_text() == "foo bar baz"
