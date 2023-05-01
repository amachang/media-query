import pytest
from typing import List, Any
from os import path
from .utils import fake_spider, fake_response
from pathlib import Path
from media_scrapy.spiders import *
from media_scrapy.errors import MediaScrapyError
from media_scrapy.items import DownloadUrlItem, SaveFileContentItem
from scrapy.http import Request, FormRequest
from typeguard import TypeCheckError

site_config_dir = path.join(path.dirname(__file__), "site_configs")


def test_main_spider_init() -> None:
    class SiteConfigDef:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure: List[Any] = []

    MainSpider(siteconf=SiteConfigDef)


def test_main_spider_init_by_file() -> None:
    MainSpider(siteconf=path.join(site_config_dir, "site_config_000.py"))
    MainSpider(siteconf=Path(site_config_dir).joinpath("site_config_000.py"))


def test_main_spider_init_error() -> None:
    with pytest.raises(MediaScrapyError):
        MainSpider(siteconf=path.join(site_config_dir, "invalid_extension.txt"))

    with pytest.raises(MediaScrapyError):
        MainSpider(siteconf=path.join(site_config_dir, "not_found.py"))

    try:
        syntax_error_site_config_path = Path(site_config_dir).joinpath(
            "syntax_error.py"
        )
        assert not syntax_error_site_config_path.exists()
        syntax_error_site_config_path.write_text("foo bar")

        assert syntax_error_site_config_path.exists()
        with pytest.raises(MediaScrapyError):
            MainSpider(siteconf=syntax_error_site_config_path)
    finally:
        syntax_error_site_config_path.unlink()
    assert not syntax_error_site_config_path.exists()

    with pytest.raises(MediaScrapyError):
        MainSpider(siteconf=path.join(site_config_dir, "site_config_no_class.py"))

    with pytest.raises(MediaScrapyError):
        MainSpider(siteconf=path.join(site_config_dir, "site_config_duplicated.py"))


def test_main_spider_start_requests() -> None:
    class SiteConfigDef000:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure: List[Any] = []

    spider = MainSpider(siteconf=SiteConfigDef000)
    req = next(spider.start_requests())
    assert req.url == "http://example.com/"
    assert req.callback == spider.parse

    class SiteConfigDef001:
        start_url = "http://example.com/"
        login = {
            "url": "http://example.com/login",
            "formdata": {
                "user": "foo",
                "password": "bar",
            },
        }
        save_dir = "/tmp"
        structure: List[Any] = []

    spider = MainSpider(siteconf=SiteConfigDef001)
    req = next(spider.start_requests())
    assert req.url == "http://example.com/"
    assert req.callback == spider.login


def test_main_spider_login() -> None:
    class SiteConfigDef:
        start_url = "http://example.com/"
        login = {
            "url": "http://example.com/login",
            "formdata": {
                "user": "foo",
                "password": "bar",
            },
        }
        save_dir = "/tmp"
        structure: List[Any] = []

    spider = MainSpider(siteconf=SiteConfigDef)
    res = fake_response()
    req = next(spider.login(res))
    assert isinstance(req, FormRequest)
    assert req.url == "http://example.com/login"
    assert req.body in {b"user=foo&password=bar", b"password=bar&user=foo"}
    assert req.callback == spider.parse_login

    login_res = fake_response(request=req)
    after_login_req = next(spider.parse_login(res))
    assert after_login_req.url == "http://example.com/"
    assert after_login_req.callback == spider.parse


def test_main_spider_parse() -> None:
    class SiteConfigDef:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure: List[Any] = [
            r"http://example\.com/",
            {
                "url": r"http://example\.com/\w+_dir",
                "file_path": lambda res: res.xpath("//title/text()").get(
                    default="unknown"
                ),
            },
            r"http://example\.com/\w+_dir/noname_dir",
            {
                "url": r"http://example\.com/files/(\w+\.txt)",
                "file_path": r"\g<1>",
            },
        ]

    spider = MainSpider(siteconf=SiteConfigDef)
    res = fake_response(body=b"<a href='/aaa_dir'>dir1</a><a href='/bbb_dir'>dir1</a>")
    results = list(spider.parse(res))
    assert len(results) == 2
    a_req, b_req = results

    assert isinstance(a_req, Request)
    assert a_req.callback == spider.parse
    assert a_req.url == "http://example.com/aaa_dir"
    assert a_req.meta["url_info"].structure_path == [0, 0]
    assert a_req.meta["url_info"].file_path == ""

    a_res = fake_response(
        request=a_req,
        body=b"<a href='/aaa_dir/xxx'>link1</a><a href='/aaa_dir/xxx'>link2</a>",
    )

    results = list(spider.parse(a_res))
    len(results) == 0

    assert isinstance(b_req, Request)
    assert b_req.callback == spider.parse
    assert b_req.url == "http://example.com/bbb_dir"
    assert b_req.meta["url_info"].structure_path == [0, 0]
    assert b_req.meta["url_info"].file_path == ""

    b_res = fake_response(
        request=b_req,
        body=b"<title>foo</title><a href='/bbb_dir/noname_dir'>link1</a><a href='/bbb_dir/noname_dir'>link2</a>",
    )

    results = list(spider.parse(b_res))
    assert len(results) == 1
    a_req = results[0]

    assert isinstance(a_req, Request)
    assert a_req.callback == spider.parse
    assert a_req.url == "http://example.com/bbb_dir/noname_dir"
    assert a_req.meta["url_info"].structure_path == [0, 0, 0]
    assert a_req.meta["url_info"].file_path == "foo"

    res = fake_response(
        request=a_req,
        body=b"<a href='/files/aaa.txt'>link1</a><a href='/files/bbb.txt'>link2</a><a href='/files/ccc.txt'>link3</a>",
    )
    results = list(spider.parse(res))
    assert len(results) == 3
    assert all(isinstance(item, DownloadUrlItem) for item in results)
    assert [item["url"] for item in results] == [
        "http://example.com/files/aaa.txt",
        "http://example.com/files/bbb.txt",
        "http://example.com/files/ccc.txt",
    ]
    assert [item["file_path"] for item in results] == [
        "/tmp/foo/aaa.txt",
        "/tmp/foo/bbb.txt",
        "/tmp/foo/ccc.txt",
    ]
