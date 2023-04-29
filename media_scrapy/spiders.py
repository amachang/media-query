import json
from typing import Dict, List, Any, Union, Optional, Type, Iterator
from pathlib import Path
import re
from importlib.machinery import SourceFileLoader
import inspect
from os import path
import os
import scrapy
from scrapy.http import Request, FormRequest, Response
from media_scrapy.errors import MediaScrapyError
from media_scrapy.conf import (
    SiteConfigDefinition,
    SiteConfig,
    DownloadUrlInfo,
    FileContentUrlInfo,
    ParseUrlInfo,
)
from media_scrapy.items import MediaFiles
from typeguard import typechecked


@typechecked
class MainSpider(scrapy.Spider):
    name = "main"

    def __init__(self, siteconf: Union[str, Path, Type[SiteConfigDefinition]]) -> None:
        super().__init__(siteconf=siteconf)

        if inspect.isclass(siteconf):
            site_conf_cls = siteconf
        else:
            if isinstance(siteconf, str):
                site_conf_path = Path(siteconf)
            else:
                assert isinstance(siteconf, Path)
                site_conf_path = siteconf

            site_conf_matches = re.search(f"(.*)\\.py$", site_conf_path.name)
            if site_conf_matches is None:
                raise MediaScrapyError(
                    f"Site config file must be a python file: {site_conf_path}"
                )

            if not site_conf_path.exists():
                raise MediaScrapyError(f"Site config file not found: {site_conf_path}")

            site_conf_modulename = site_conf_matches.group(1)
            site_conf_module_loader = SourceFileLoader(
                site_conf_modulename, str(site_conf_path)
            )

            try:
                site_conf_module = site_conf_module_loader.load_module()
            except SyntaxError as err:
                raise MediaScrapyError(
                    f"Invalid python syntax in site config: {site_conf_path}"
                ) from err

            site_conf_cls_candidates = list(
                filter(inspect.isclass, vars(site_conf_module).values())
            )

            def is_site_config_def(cls: Type) -> bool:
                assert hasattr(cls, "__name__")
                return re.search(r"SiteConfig", cls.__name__) is not None

            site_conf_cls_candidates = list(
                filter(is_site_config_def, site_conf_cls_candidates)
            )

            if len(site_conf_cls_candidates) < 1:
                raise MediaScrapyError(
                    f"Class not found in site config: {site_conf_path}"
                )

            if 1 < len(site_conf_cls_candidates):
                raise MediaScrapyError(
                    f"Too many classes in site config: {site_conf_cls_candidates}"
                )

            site_conf_cls = site_conf_cls_candidates[0]

        site_conf = site_conf_cls()
        self.config = SiteConfig(site_conf)

    def start_requests(self) -> Iterator[Request]:
        if self.config.needs_login:
            callback = self.login
        else:
            callback = self.parse
        yield Request(
            self.config.start_url,
            callback=callback,
            dont_filter=True,
            meta={"structure_path": [], "file_path": []},
        )

    def login(self, res: Response) -> Iterator[Request]:
        assert self.config.needs_login
        yield FormRequest(
            self.config.login.url,
            formdata=self.config.login.formdata,
            callback=self.parse_login,
        )

    def parse_login(self, res: Response) -> Iterator[Request]:
        yield Request(
            self.config.start_url,
            callback=self.parse,
            dont_filter=True,
            meta={"structure_path": [], "file_path": []},
        )

    def parse(self, res: Response) -> Iterator[Union[Request, MediaFiles]]:
        url_infos = self.config.get_url_infos(res)

        file_urls: List[str] = []
        file_contents: List[Optional[bytes]] = []
        file_paths: List[str] = []
        download_url_seen = set()
        for url_info in url_infos:
            url = url_info.url
            if isinstance(url_info, FileContentUrlInfo):
                file_urls.append(url)
                file_contents.append(url_info.file_content)
                file_paths.append(path.join(self.config.save_dir, url_info.file_path))
            elif isinstance(url_info, DownloadUrlInfo):
                if url in download_url_seen:
                    continue
                download_url_seen.add(url)
                file_urls.append(url)
                file_contents.append(None)
                file_paths.append(path.join(self.config.save_dir, url_info.file_path))
            elif isinstance(url_info, ParseUrlInfo):
                yield Request(url, callback=self.parse, meta={"url_info": url_info})
            else:
                assert False

        file_paths = [path.abspath(file_path) for file_path in file_paths]

        if 0 < len(file_urls):
            yield MediaFiles(
                file_urls=file_urls, file_contents=file_contents, file_paths=file_paths
            )
