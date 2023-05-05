import json
from typing import Dict, List, Any, Union, Optional, Type, Iterator, Callable
from pathlib import Path
import re
import inspect
from os import path
import os
import scrapy
from scrapy.http import Request, FormRequest, Response
from media_scrapy.errors import MediaScrapyError
from media_scrapy.conf import (
    SiteConfig,
    UrlCommand,
    DownloadUrlCommand,
    SaveFileContentCommand,
    RequestUrlCommand,
)
from media_scrapy.items import DownloadUrlItem, SaveFileContentItem
from typeguard import typechecked


@typechecked
class SpiderBase(scrapy.Spider):
    def __init__(self, config: SiteConfig) -> None:
        super().__init__()
        self.config = config

    def start_requests(self) -> Iterator[Request]:
        if self.config.needs_login:
            yield self.get_start_request_before_login()
        else:
            yield self.get_first_request()

    def get_start_request_before_login(self) -> Request:
        return self.get_start_request(self.login)

    def login(self, res: Response) -> Iterator[Request]:
        assert self.config.needs_login
        if self.config.login.formdata is not None:
            yield FormRequest(
                self.config.login.url,
                formdata=self.config.login.formdata,
                callback=self.parse_login,
            )
        else:
            yield Request(
                self.config.login.url,
                dont_filter=True,
                callback=self.parse_login,
            )

    def parse_login(self, res: Response) -> Iterator[Request]:
        yield self.get_first_request()

    def get_start_request(
        self,
        callback: Callable[[Response], Any],
    ) -> Request:
        command = self.config.get_start_command()
        return self.get_request_for_command(command, callback, True)

    def get_item_for_command(self, command: UrlCommand) -> Optional[scrapy.Item]:
        if isinstance(command, SaveFileContentCommand):
            return SaveFileContentItem(
                file_content=command.file_content,
                file_path=path.abspath(
                    path.join(self.config.save_dir, command.file_path)
                ),
            )

        elif isinstance(command, DownloadUrlCommand):
            return DownloadUrlItem(
                url=command.url,
                file_path=path.abspath(
                    path.join(self.config.save_dir, command.file_path)
                ),
            )
        else:
            return None

    def get_request_for_command(
        self,
        command: UrlCommand,
        callback: Callable[[Response], Any],
        dont_filter: bool = False,
    ) -> Optional[Request]:
        if isinstance(command, RequestUrlCommand):
            return Request(
                command.url_info.url,
                callback=callback,
                dont_filter=dont_filter,
                meta={"url_info": command.url_info},
            )
        else:
            return None

    def get_first_request(self) -> Request:
        raise NotImplementedError()


@typechecked
class MainSpider(SpiderBase):
    name = "main"

    def get_first_request(self) -> Request:
        return self.get_start_request(self.parse)

    def parse(self, res: Response) -> Iterator[Union[Request, scrapy.Item]]:
        commands = self.config.get_url_commands(res, res.meta["url_info"])

        for command in commands:
            if item := self.get_item_for_command(command):
                yield item
            if req := self.get_request_for_command(command, self.parse):
                yield req


@typechecked
class DebugSpider(SpiderBase):
    name = "debug"

    def __init__(
        self,
        config: SiteConfig,
        debug_target_url: str,
        choose_structure_definitions_callback: Callable[[List[str]], int],
        start_debug_callback: Callable[[Dict[str, Any]], None],
    ) -> None:
        super().__init__(config)
        self.config = config
        self.debug_target_url = debug_target_url
        self.choose_structure_definitions = choose_structure_definitions_callback
        self.start_debug = start_debug_callback

    def get_start_request_before_login(self) -> Request:
        request = super().get_start_request_before_login()
        self.logger.info(f"Requesting top page...: {request.url}")
        return request

    def login(self, res: Response) -> Iterator[Request]:
        self.logger.info(f"Logging in...: {self.config.login.url}")
        return super().login(res)

    def get_first_request(self) -> Request:
        command_candidates = self.config.get_simulated_command_candidates_for_url(
            self.debug_target_url
        )
        if len(command_candidates) == 0:
            raise MediaScrapyError(f"No structures for url: {self.debug_target_url}")

        if len(command_candidates) == 1:
            command_index = 0
        else:
            command_index = self.choose_structure_definitions(
                [structure_desc for structure_desc, command in command_candidates]
            )

        structure_desc, command = command_candidates[command_index]
        self.logger.info(f"Requesting...: {command.url_info.url}")

        return self.get_request_for_command(command, self.parse, True)

    def parse(self, res: Response) -> None:
        debug_env = self.config.get_debug_environment(res, res.meta["url_info"])
        self.start_debug(debug_env)
