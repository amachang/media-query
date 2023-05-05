from scrapy.http import Response, HtmlResponse, Request
from typing import Type, List, Any, Optional, Dict
from pathlib import Path
from scrapy.settings import Settings
from media_scrapy import settings as setting_definitions
from media_scrapy.spiders import MainSpider
from media_scrapy.conf import SiteConfig, UrlInfo
from dill.source import dumpsource


def make_temporary_site_config_file(tmpdir: Path, cls: Type) -> Path:
    file_path = tmpdir.joinpath(f"{cls.__name__}.py")
    assert not file_path.exists()
    file_path.write_text(dumpsource(cls, alias=cls.__name__, enclose=False))
    assert file_path.exists()
    return file_path


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

    if "url_info" not in request.meta:
        request.meta["url_info"] = UrlInfo(request.url)

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
    spider = MainSpider(config=SiteConfig.create_by_definition(SiteConfigDef))
    spider.settings = settings

    return spider
