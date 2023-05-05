from typing import Any, List
from pathlib import Path
from .utils import make_temporary_site_config_file
from media_scrapy.__main__ import main_command
from pytest_httpserver import HTTPServer


def test_main_command(tmpdir: Any) -> None:
    """
    Since reactors cannot be rerun once run, this test, including stop and run,
    cannot be included in the same test file as other tests.
    """
    tmpdir = Path(tmpdir)
    with HTTPServer() as httpserver:
        httpserver.expect_request("/start").respond_with_data(
            "<a href='/a'>link</a>", content_type="text/html"
        )

        class SiteConfigDef:
            start_url = httpserver.url_for("/start")
            save_dir = str(tmpdir)
            structure: List[Any] = []

        site_config_path = make_temporary_site_config_file(tmpdir, SiteConfigDef)
        main_command(["-c", str(site_config_path)], standalone_mode=False)
