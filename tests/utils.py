from scrapy.http import Response, HtmlResponse, Request
from typing import List, Any, Optional, Dict
from pathlib import Path
from scrapy.settings import Settings
from media_scrapy import settings as setting_definitions
from media_scrapy.spiders import MainSpider
from media_scrapy.conf import UrlInfo


def fake_response(
    url: str = "http://example.com/",
    body: bytes = b"<body>foo</body>",
    url_info: Optional[UrlInfo] = None,
    request: Optional[Request] = None,
) -> Response:
    if request is None:
        request = Request(url=url)
    assert request is not None
    if url_info is not None:
        request.meta["url_info"] = url_info
    response = HtmlResponse(url=url, request=request, body=body)
    for key, value in request.meta.items():
        response.meta[key] = value
    return response


def fake_spider(
    tmpdir: Path, additional_settings: Optional[Dict[str, Any]] = None
) -> MainSpider:
    class SiteConfigDef:
        start_url = "http://example.com/"
        save_dir = str(Path(tmpdir))
        structure: List[Any] = []

    settings = Settings()
    settings.setmodule(setting_definitions, priority="project")
    if additional_settings is not None:
        settings.setdict(additional_settings, priority="cmdline")
    spider = MainSpider(siteconf=SiteConfigDef)
    spider.settings = settings

    return spider
