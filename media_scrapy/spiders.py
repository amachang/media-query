import json
from typing import Dict, List, Any, Union, Optional, Type, Iterable
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
    FileSiteUrlInfo,
    DirSiteUrlInfo,
    NoDirSiteUrlInfo,
)
from media_scrapy.items import MediaFiles


class MainSpider(scrapy.Spider):
    name = "main"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if "siteconf" not in kwargs:
            raise MediaScrapyError(f"Site config path not given")

        site_conf_path_str: Union[str, Path, Type[SiteConfigDefinition]] = kwargs[
            "siteconf"
        ]
        if inspect.isclass(site_conf_path_str):
            site_conf_cls = site_conf_path_str
        else:
            if isinstance(site_conf_path_str, str):
                site_conf_path = Path(site_conf_path_str)
            else:
                assert isinstance(site_conf_path_str, Path)
                site_conf_path = site_conf_path_str

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

    def start_requests(self) -> Request:
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

    def login(self, res: Response) -> Request:
        assert self.config.needs_login
        yield FormRequest(
            self.config.login.url,
            formdata=self.config.login.formdata,
            callback=self.parse_login,
        )

    def parse_login(self, res: Response) -> Request:
        yield Request(
            self.config.start_url,
            callback=self.parse,
            dont_filter=True,
            meta={"structure_path": [], "file_path": []},
        )

    def parse(self, res: Response) -> Iterable[Union[Request, MediaFiles]]:
        url_infos = self.config.get_url_infos(res)

        file_urls = []
        file_paths = []
        for url_info in url_infos:
            url = url_info.url
            if isinstance(url_info, FileSiteUrlInfo):
                file_urls.append(url)
                file_path_components_list = url_info.file_path
                file_path = path.join(*file_path_components_list)
                file_paths.append(path.join(self.config.save_dir, file_path))
            elif isinstance(url_info, DirSiteUrlInfo):
                yield Request(
                    url,
                    callback=self.parse_dir,
                    meta={
                        "structure_path": url_info.structure_path,
                        "parent_file_path": url_info.parent_file_path,
                    },
                )
            else:
                assert isinstance(url_info, NoDirSiteUrlInfo)
                yield Request(
                    url,
                    callback=self.parse,
                    meta={
                        "structure_path": url_info.structure_path,
                        "file_path": url_info.file_path,
                    },
                )

        yield MediaFiles(file_urls=file_urls, file_paths=file_paths)

    def parse_dir(
        self, res: Response
    ) -> Iterable[Optional[Union[Request, MediaFiles]]]:
        parent_file_path = res.meta["parent_file_path"]
        dirname = self.config.get_dirname(res)
        if dirname is not None:
            file_path = parent_file_path + [dirname]
            res.meta["file_path"] = file_path
            assert res.meta["file_path"] == file_path
            return self.parse(res)
        else:
            return []
