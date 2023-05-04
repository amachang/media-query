import pytest
from os import path
from pathlib import Path
from typing import List, Any
from media_scrapy.__main__ import *
from pytest_httpserver import HTTPServer


class TestError(Exception):
    pass


def test_main(tmpdir: Any) -> None:
    with HTTPServer() as httpserver:
        httpserver.expect_request("/start").respond_with_data(
            "<a href='/a'>link</a>", content_type="text/html"
        )

        class SiteConfigDef:
            start_url = httpserver.url_for("/start")
            save_dir = str(Path(tmpdir))
            structure: List[Any] = []

        main(SiteConfigDef, False, None)


def test_main_init_error(tmpdir: Any) -> None:
    class SiteConfigDef:
        @property
        def start_url(self) -> str:
            raise TestError("test")

        save_dir = str(Path(tmpdir))
        structure: List[Any] = []

    with pytest.raises(TestError):
        main(SiteConfigDef, False, None)
