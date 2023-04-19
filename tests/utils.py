from scrapy.http import Response, HtmlResponse, Request
from typing import List, Any, Optional
from pathlib import Path
from scrapy.settings import Settings
from media_scrapy import settings as setting_definitions
from media_scrapy.spiders import MainSpider


def fake_response(
    url: str = "http://example.com/",
    body: bytes = b"<body>foo</body>",
    structure_path: List[int] = [],
    file_path: List[str] = [],
    request: Optional[Request] = None,
) -> Response:
    if request is None:
        request = Request(url=url)
        request.meta["structure_path"] = structure_path
        request.meta["file_path"] = file_path
    assert request is not None
    response = HtmlResponse(url=url, request=request, body=body)
    for key, value in request.meta.items():
        response.meta[key] = value
    return response


def fake_spider(tmpdir: Path) -> MainSpider:
    class SiteConfigDef:
        start_url = "http://example.com/"
        save_dir = str(Path(tmpdir))
        structure: List[Any] = []

    settings = Settings()
    settings.setmodule(setting_definitions, priority="project")
    spider = MainSpider(siteconf=SiteConfigDef)
    spider.settings = settings

    return spider
