import pytest
from os import path
from pathlib import Path
from typing import List, Any, cast
from media_scrapy.__main__ import (
    main,
    choose_structure_definitions,
    start_ipython_process,
    start_debug_repl,
    run_until_done,
)
from pytest_httpserver import HTTPServer
from .utils import make_temporary_site_config_file
from pytest_mock.plugin import MockerFixture
from twisted.internet.defer import Deferred
from scrapy.crawler import CrawlerRunner
from media_scrapy.spiders import MainSpider, DebugSpider
from media_scrapy.conf import SiteConfig
import click
from unittest.mock import Mock
from threading import Thread
from twisted.internet.base import ReactorBase
from twisted.internet.error import ReactorNotRunning
from twisted.python.failure import Failure

site_config_dir = path.join(path.dirname(__file__), "site_configs")


class TestError(Exception):
    pass


def test_main_spider(mocker: MockerFixture, tmpdir: Any) -> None:
    mocker.patch("scrapy.crawler.CrawlerRunner.crawl", return_value=Deferred())

    with HTTPServer() as httpserver:
        httpserver.expect_request("/start").respond_with_data(
            "<a href='/a'>link</a>", content_type="text/html"
        )

        class SiteConfigDef:
            start_url = httpserver.url_for("/start")
            save_dir = str(Path(tmpdir))
            structure: List[Any] = [{"url": r"http://example\.com/start"}]

        main(SiteConfigDef, False, None)

    mock = cast(Mock, CrawlerRunner.crawl)
    mock.assert_called_once()
    assert mock.call_args.args[0] == MainSpider
    assert isinstance(mock.call_args.kwargs["config"], SiteConfig)


def test_debug_spider(mocker: MockerFixture, tmpdir: Any) -> None:
    mocker.patch("scrapy.crawler.CrawlerRunner.crawl", return_value=Deferred())

    with HTTPServer() as httpserver:
        httpserver.expect_request("/start").respond_with_data(
            "<a href='/a'>link</a>", content_type="text/html"
        )

        class SiteConfigDef:
            start_url = httpserver.url_for("/start")
            save_dir = str(Path(tmpdir))
            structure: List[Any] = [
                {"url": r"http://example\.com/start"},
                {"url": r"http://example\.com/test_target"},
            ]

        main(SiteConfigDef, False, "http://example.com/test_target")

    mock = cast(Mock, CrawlerRunner.crawl)
    mock.assert_called_once()
    assert mock.call_args.args[0] == DebugSpider
    assert isinstance(mock.call_args.kwargs["config"], SiteConfig)
    assert callable(mock.call_args.kwargs["choose_structure_definitions_callback"])
    assert callable(mock.call_args.kwargs["start_debug_callback"])


def test_main_init_error(tmpdir: Any) -> None:
    class SiteConfigDef:
        @property
        def start_url(self) -> str:
            raise TestError("test")

        save_dir = str(Path(tmpdir))
        structure: List[Any] = []

    with pytest.raises(TestError):
        main(SiteConfigDef, False, None)


def test_choose_structure_definitions(mocker: MockerFixture) -> None:
    mocker.patch("click.prompt", return_value=2)

    assert (
        choose_structure_definitions(["{}", '{ "url" "http://example\\\\.com/" }']) == 1
    )
    mock = cast(Mock, click.prompt)
    mock.assert_called_once()
    assert isinstance(mock.call_args.args[0], str)
    assert isinstance(mock.call_args.kwargs["type"], click.IntRange)


def test_start_debug_repl(mocker: MockerFixture) -> None:
    mocker.patch("IPython.start_ipython")
    from IPython import start_ipython

    start_debug_repl({"foo": "bar"})

    mock = cast(Mock, start_ipython)
    mock.assert_called_once()
    assert mock.call_args.kwargs == {"argv": [], "user_ns": {"foo": "bar"}}


def test_run_until_done(mocker: MockerFixture) -> None:
    mocker.patch("twisted.internet.reactor.run")
    mocker.patch("twisted.internet.reactor.stop")
    import twisted.internet.reactor

    reactor = cast(ReactorBase, twisted.internet.reactor)

    deferred: Deferred[None] = Deferred()
    run_until_done(deferred)

    run_mock = cast(Mock, reactor.run)
    stop_mock = cast(Mock, reactor.stop)

    run_mock.assert_called_once_with()
    assert not stop_mock.called

    deferred.callback(None)

    stop_mock.assert_called_once_with()


def test_run_until_done_reactor_not_running(mocker: MockerFixture) -> None:
    mocker.patch("twisted.internet.reactor.run")
    mocker.patch("twisted.internet.reactor.stop", side_effect=ReactorNotRunning)
    import twisted.internet.reactor

    reactor = cast(ReactorBase, twisted.internet.reactor)

    deferred: Deferred[None] = Deferred()
    run_until_done(deferred)

    run_mock = cast(Mock, reactor.run)
    stop_mock = cast(Mock, reactor.stop)

    run_mock.assert_called_once_with()
    assert not stop_mock.called

    deferred.callback(None)

    stop_mock.assert_called_once_with()


def test_run_until_done_errorback(mocker: MockerFixture) -> None:
    deferred: Deferred[None] = Deferred()

    mocker.patch(
        "twisted.internet.reactor.run",
        side_effect=lambda: deferred.errback(TestError()),
    )
    mocker.patch("twisted.internet.reactor.stop")
    import twisted.internet.reactor

    reactor = cast(ReactorBase, twisted.internet.reactor)

    with pytest.raises(TestError):
        run_until_done(deferred)

    run_mock = cast(Mock, reactor.run)
    stop_mock = cast(Mock, reactor.stop)

    run_mock.assert_called_once_with()
    stop_mock.assert_called_once_with()
