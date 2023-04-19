import pytest
from urllib.parse import urlparse
from media_scrapy.conf import *
from typing import Union, List, cast
from .utils import fake_response
from dataclasses import dataclass, field


@dataclass
class ConfDef:
    start_url: str
    save_dir: str
    structure: list
    login: dict = field(
        default_factory=lambda: {"url": "http://example.com/", "formdata": {}}
    )
    ignore_url: Union[str, List[str]] = field(
        default_factory=lambda: cast(List[str], [])
    )


def test_site_config_init() -> None:
    @dataclass
    class ConfDef0:
        start_url: str
        save_dir: str
        structure: list

    config = SiteConfig(
        ConfDef0(start_url="http://example.com", save_dir="/tmp", structure=[])
    )

    @dataclass
    class ConfDef1:
        start_url: str
        save_dir: str
        structure: list
        login: dict

    config = SiteConfig(
        ConfDef1(
            start_url="http://example.com",
            save_dir="/tmp",
            structure=[],
            login={
                "url": "http://example.com/login",
                "formdata": {"email": "foo", "password": "bar"},
            },
        )
    )

    @dataclass
    class ConfDef2:
        start_url: str
        save_dir: str
        structure: list
        ignore_url: Union[str, List[str]]

    config = SiteConfig(
        ConfDef2(
            start_url="http://example.com",
            save_dir="/tmp",
            structure=[],
            ignore_url=r"http://example\.com/",
        )
    )


def test_site_config_get_dirname() -> None:
    config = SiteConfig(
        ConfDef(
            start_url="http://example.com",
            save_dir="/tmp",
            structure=[
                {
                    "url": r"http://example\.com/a",
                    "dirname": lambda res: res.xpath("//p[@class='bar']/text()").get(),
                },
            ],
        )
    )
    res = fake_response(body=b"<p>foo</p><p class='bar'>baz</p>", structure_path=[0])
    assert config.get_dirname(res) == "baz"


def test_site_config_get_url_infos_unknown() -> None:
    config = SiteConfig(
        ConfDef(
            start_url="http://example.com",
            save_dir="/tmp",
            structure=[
                {
                    "url": r"http://example\.com/",
                    "dirname": lambda res: res.xpath("//p[@class='bar']/text()").get(),
                },
            ],
        )
    )
    res = fake_response(body=b"<a href='http://example.com/unknown'>foo</a>")

    with pytest.raises(MediaScrapyError):
        url_infos = config.get_url_infos(res)


def test_site_config_get_url_infos_dir() -> None:
    config = SiteConfig(
        ConfDef(
            start_url="http://example.com",
            save_dir="/tmp",
            structure=[
                {
                    "url": r"http://example\.com/",
                    "dirname": lambda res: res.xpath("//p[@class='bar']/text()").get(),
                },
            ],
        )
    )
    res = fake_response(body=b"<a href='http://example.com/'>foo</a>")
    url_infos = config.get_url_infos(res)
    assert len(url_infos) == 1
    url_info = url_infos[0]
    assert isinstance(url_info, DirSiteUrlInfo)
    assert url_info.url == "http://example.com/"
    assert url_info.structure_path == [0]
    assert url_info.parent_file_path == []


def test_site_config_get_url_infos_dupe_urls() -> None:
    config = SiteConfig(
        ConfDef(
            start_url="http://example.com",
            save_dir="/tmp",
            structure=[
                {
                    "url": r"http://example\.com/a",
                    "dirname": lambda res: res.xpath("//p[@class='bar']/text()").get(),
                },
            ],
        )
    )
    res = fake_response(
        url="http://example.com/", body=b"<a href='a'>foo</a><img src='/a'>"
    )
    url_infos = config.get_url_infos(res)
    assert len(url_infos) == 1
    url_info = url_infos[0]
    assert isinstance(url_info, DirSiteUrlInfo)
    assert url_info.url == "http://example.com/a"
    assert url_info.structure_path == [0]
    assert url_info.parent_file_path == []


def test_site_config_get_url_infos_ignore_url() -> None:
    config = SiteConfig(
        ConfDef(
            start_url="http://example.com",
            save_dir="/tmp",
            structure=[
                {
                    "url": r"http://example\.com/a",
                    "dirname": lambda res: res.xpath("//p[@class='bar']/text()").get(),
                },
            ],
            ignore_url=r"http://example\.com/a",
        )
    )
    res = fake_response(body=b"<a href='http://example.com/a'>foo</a>")
    url_infos = config.get_url_infos(res)
    assert len(url_infos) == 0


def test_site_config_get_url_infos_nodir() -> None:
    config = SiteConfig(
        ConfDef(
            start_url="http://example.com",
            save_dir="/tmp",
            structure=[
                r"http://example\.com/a",
            ],
        )
    )
    res = fake_response(body=b"<a href='http://example.com/a'>foo</a>")
    url_infos = config.get_url_infos(res)
    assert len(url_infos) == 1
    url_info = url_infos[0]
    assert isinstance(url_info, NoDirSiteUrlInfo)
    assert url_info.url == "http://example.com/a"
    assert url_info.structure_path == [0]
    assert url_info.file_path == []


def test_site_config_get_url_infos_file() -> None:
    config = SiteConfig(
        ConfDef(
            start_url="http://example.com",
            save_dir="/tmp",
            structure=[
                {"url": r"http://example\.com/a\.jpg", "filename": r"^.*/([^/]+)$"},
            ],
        )
    )
    res = fake_response(body=b"<img src='http://example.com/a.jpg'>")
    url_infos = config.get_url_infos(res)
    assert len(url_infos) == 1
    url_info = url_infos[0]
    assert isinstance(url_info, FileSiteUrlInfo)
    assert url_info.url == "http://example.com/a.jpg"
    assert url_info.structure_path == [0]
    assert url_info.file_path == ["a.jpg"]


def test_site_config_get_url_infos_dir_and_file() -> None:
    config = SiteConfig(
        ConfDef(
            start_url="http://example.com",
            save_dir="/tmp",
            structure=[
                {
                    "url": r"http://example\.com/a/(\?page=\d+)?",
                    "dirname": lambda res: res.xapth("//title/text()").get(),
                },
                {
                    "url": r"http://example\.com/a/\w+",
                    "filename": r"^.*/([^/]+)$",
                },
            ],
        )
    )
    res = fake_response(
        url="http://example.com/a/",
        body=b"<title>foo</title><a href='/a/'>0</a><a href='/a/?page=2'>1</a><a href='/a/?page=3'>2</a><a href='/a/file'>file</a>",
        structure_path=[0],
        file_path=["foo"],
    )
    url_infos = config.get_url_infos(res)
    assert len(url_infos) == 1
    assert isinstance(url_infos[0], FileSiteUrlInfo)
    assert url_infos[0].url == "http://example.com/a/file"
    assert url_infos[0].structure_path == [0, 0]
    assert url_infos[0].file_path == ["foo", "file"]


def test_site_config_get_url_infos_dir_paging() -> None:
    config = SiteConfig(
        ConfDef(
            start_url="http://example.com",
            save_dir="/tmp",
            structure=[
                {
                    "url": r"http://example\.com/a/(\?page=\d+)?",
                    "dirname": lambda res: res.xapth("//title/text()").get(),
                    "paging": True,
                },
                {
                    "url": r"http://example\.com/a/\w+",
                    "filename": r"^.*/([^/]+)$",
                },
            ],
        )
    )
    res = fake_response(
        url="http://example.com/a/",
        body=b"<title>foo</title><a href='/a/'>0</a><a href='/a/?page=2'>1</a><a href='/a/?page=3'>2</a><a href='/a/file'>file</a>",
        structure_path=[0],
        file_path=["foo"],
    )
    url_infos = config.get_url_infos(res)
    assert len(url_infos) == 4
    assert isinstance(url_infos[0], DirSiteUrlInfo)
    assert url_infos[0].url == "http://example.com/a/"
    assert url_infos[0].structure_path == [0]
    assert url_infos[0].parent_file_path == []
    assert isinstance(url_infos[1], DirSiteUrlInfo)
    assert url_infos[1].url == "http://example.com/a/?page=2"
    assert url_infos[1].structure_path == [0]
    assert url_infos[1].parent_file_path == []
    assert isinstance(url_infos[2], DirSiteUrlInfo)
    assert url_infos[2].url == "http://example.com/a/?page=3"
    assert url_infos[2].structure_path == [0]
    assert url_infos[2].parent_file_path == []
    assert isinstance(url_infos[3], FileSiteUrlInfo)
    assert url_infos[3].url == "http://example.com/a/file"
    assert url_infos[3].structure_path == [0, 0]
    assert url_infos[3].file_path == ["foo", "file"]


def test_site_config_get_url_infos_nodir_paging() -> None:
    config = SiteConfig(
        ConfDef(
            start_url="http://example.com",
            save_dir="/tmp",
            structure=[
                {
                    "url": r"http://example\.com/a/(\?page=\d+)?",
                    "paging": True,
                },
                {
                    "url": r"http://example\.com/a/\w+",
                    "filename": r"^.*/([^/]+)$",
                },
            ],
        )
    )
    res = fake_response(
        url="http://example.com/a/",
        body=b"<a href='/a/'>0</a><a href='/a/?page=2'>1</a><a href='/a/?page=3'>2</a><a href='/a/file'>file</a>",
        structure_path=[0],
        file_path=[],
    )
    url_infos = config.get_url_infos(res)
    assert len(url_infos) == 4
    assert isinstance(url_infos[0], NoDirSiteUrlInfo)
    assert url_infos[0].url == "http://example.com/a/"
    assert url_infos[0].structure_path == [0]
    assert url_infos[0].file_path == []
    assert isinstance(url_infos[1], NoDirSiteUrlInfo)
    assert url_infos[1].url == "http://example.com/a/?page=2"
    assert url_infos[1].structure_path == [0]
    assert url_infos[1].file_path == []
    assert isinstance(url_infos[2], NoDirSiteUrlInfo)
    assert url_infos[2].url == "http://example.com/a/?page=3"
    assert url_infos[2].structure_path == [0]
    assert url_infos[2].file_path == []
    assert isinstance(url_infos[3], FileSiteUrlInfo)
    assert url_infos[3].url == "http://example.com/a/file"
    assert url_infos[3].structure_path == [0, 0]
    assert url_infos[3].file_path == ["file"]


def test_site_config_get_url_infos_branch_structure() -> None:
    config = SiteConfig(
        ConfDef(
            start_url="http://example.com",
            save_dir="/tmp",
            structure=[
                r"http://example\.com/\w+/",
                r"http://example\.com/\w+/\w+/",
                [
                    [
                        {
                            "url": r"http://example\.com/\w+/\w+/images/",
                            "dirname": lambda res: "images",
                        },
                        {
                            "url": r"http://example\.com/\w+/\w+/images/\w+.jpg",
                            "filename": r"^.*/([^/]+)$",
                        },
                    ],
                    [
                        {
                            "url": r"http://example\.com/\w+/\w+/movies/",
                            "dirname": lambda res: "movies",
                        },
                        {
                            "url": r"http://example\.com/\w+/\w+/movies/\w+.mp4",
                            "filename": r"^.*/([^/]+)$",
                        },
                    ],
                ],
            ],
        )
    )

    res = fake_response(
        url="http://example.com/foo/bar/",
        body=b"<a href='/foo/bar/images/'>images</a><a href='/foo/bar/movies/'>movies</a>",
        structure_path=[0, 0],
        file_path=[],
    )
    url_infos = config.get_url_infos(res)
    assert len(url_infos) == 2
    assert isinstance(url_infos[0], DirSiteUrlInfo)
    assert url_infos[0].url == "http://example.com/foo/bar/images/"
    assert url_infos[0].structure_path == [0, 0, 0]
    assert url_infos[0].parent_file_path == []
    assert isinstance(url_infos[1], DirSiteUrlInfo)
    assert url_infos[1].url == "http://example.com/foo/bar/movies/"
    assert url_infos[1].structure_path == [0, 0, 1]
    assert url_infos[1].parent_file_path == []

    res = fake_response(
        url=url_infos[1].url,
        body=b"<a href='/foo/bar/movies/file.mp4'>file</a>",
        structure_path=[0, 0, 1],
        file_path=["movie"],
    )
    url_infos = config.get_url_infos(res)
    assert len(url_infos) == 1
    assert isinstance(url_infos[0], FileSiteUrlInfo)
    assert url_infos[0].url == "http://example.com/foo/bar/movies/file.mp4"
    assert url_infos[0].structure_path == [0, 0, 1, 0]
    assert url_infos[0].file_path == ["movie", "file.mp4"]


def test_parse_structure_no_more_structure_error() -> None:
    with pytest.raises(MediaScrapyError):
        config = SiteConfig(
            ConfDef(
                start_url="http://example.com",
                save_dir="/tmp",
                structure=[
                    {
                        "url": r"http://example\.com/a\.jpg",
                        "filename": r"^.*/([^/]+)$",
                    },
                    r"http://example\.com/foo",
                ],
            )
        )

    with pytest.raises(MediaScrapyError):
        config = SiteConfig(
            ConfDef(
                start_url="http://example.com",
                save_dir="/tmp",
                structure=[
                    [
                        [r"http://example\.com/a"],
                        [r"http://example\.com/b"],
                    ],
                    r"http://example\.com/foo",
                ],
            )
        )


def test_parse_structure_type_error() -> None:
    with pytest.raises(MediaScrapyError):
        config = SiteConfig(
            ConfDef(
                start_url="http://example.com",
                save_dir="/tmp",
                structure=[
                    r"http://example\.com/foo",
                    111,
                ],
            )
        )


def test_get_filename_extractor() -> None:
    filename_extractor = get_filename_extractor(r"^.*/(aaa)$")
    assert filename_extractor("foobarbaz/aaa") == "aaa"
    with pytest.raises(MediaScrapyError):
        filename_extractor("foobarbaz/bbb")
    filename_extractor = get_filename_extractor(lambda url: url[-3:])
    assert filename_extractor("foobarbaz/aaabbb") == "bbb"
    with pytest.raises(MediaScrapyError):
        get_filename_extractor(r"(hoge]")


def test_get_dirname_extractor() -> None:
    def extractor_def(res: Response) -> str:
        return str(res.xpath('//p[@class="bar"]/text()').get())

    dirname_extractor = get_filename_extractor(extractor_def)
    res = fake_response(body=b"<p>foo</p><p class='bar'>baz</p>")
    assert dirname_extractor(res) == "baz"


def test_get_url_matcher() -> None:
    url_matcher = get_url_matcher(r"http://example\.com/.*")
    assert url_matcher("http://example.com/a")
    assert not url_matcher("https://example.com/a")

    url_matcher = get_url_matcher(
        lambda url: urlparse(url).hostname in {"a.example.com", "example.com"}
    )

    assert url_matcher("https://a.example.com/")
    assert not url_matcher("https://b.example.com/")

    url_matcher = get_url_matcher(
        [
            lambda url: urlparse(url).hostname in {"a.example.com", "example.com"},
            r"http://b.example\.com/.*",
        ]
    )

    assert url_matcher("https://a.example.com/")
    assert url_matcher("http://b.example.com/")
    assert not url_matcher("https://c.example.com/")

    with pytest.raises(MediaScrapyError):
        get_url_matcher(r"(aaa]")

    with pytest.raises(MediaScrapyError):
        get_url_matcher([111])
