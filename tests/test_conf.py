import pytest
import json
from urllib.parse import urlparse, parse_qsl, parse_qs
from media_scrapy.conf import *
from typing import Union, List, cast
from .utils import fake_response
from dataclasses import dataclass, field
from hashlib import md5

site_config_dir = path.join(path.dirname(__file__), "site_configs")


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

    config = SiteConfig(
        ConfDef2(
            start_url="http://example.com",
            save_dir="/tmp",
            structure=[],
        )
    )

    @dataclass
    class ConfDef3:
        start_url: str
        save_dir: str
        structure: list
        login: dict

    config = SiteConfig(
        ConfDef3(
            start_url="http://example.com",
            save_dir="/tmp",
            structure=[],
            login={
                "url": "http://example.com/login",
            },
        )
    )

    @dataclass
    class ConfDef4:
        start_url: str
        save_dir: str
        structure: list
        login: str

    config = SiteConfig(
        ConfDef4(
            start_url="http://example.com",
            save_dir="/tmp",
            structure=[],
            login="http://example.com/login",
        )
    )


def test_site_config_init_error() -> None:
    class ConfDef1:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {"url": "http://example\.com/"},
            [
                [{"url": "http://example\.com/foo"}],
                [{"url": "http://example\.com/bar"}],
            ],
            {"url": "http://example\.com/baz"},
        ]

    with pytest.raises(MediaScrapyError):
        SiteConfig(ConfDef1())

    class ConfDef2:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {"url": "http://example\.com/"},
            [
                [{"url": "http://example\.com/foo"}],
                [{"url": "http://example\.com/bar"}, 111],
            ],
        ]

    with pytest.raises(MediaScrapyError):
        SiteConfig(ConfDef2())

    class ConfDef3:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {"url": "http://example\.com/"},
            [
                [{"url": "http://example\.com/foo("}],
                [{"url": "http://example\.com/bar"}],
            ],
        ]

    with pytest.raises(SchemaError):
        SiteConfig(ConfDef3())

    class ConfDef4:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {"url": "http://example\.com/"},
            [
                [{"url": lambda url, another_param: True}],
                [{"url": "http://example\.com/bar"}],
            ],
        ]

    with pytest.raises(SchemaError):
        SiteConfig(ConfDef4())

    class ConfDef5:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": "http://example\.com/",
                "content": "xpat_error((([[[",
            },
            {
                "url": "http://example\.com/foo",
            },
        ]

    with pytest.raises(SchemaError):
        SiteConfig(ConfDef5())

    @dataclass
    class ConfDef6:
        start_url: str
        save_dir: str
        structure: list
        login: int

    with pytest.raises(MediaScrapyError):
        config = SiteConfig(
            ConfDef6(
                start_url="http://example.com",
                save_dir="/tmp",
                structure=[],
                login=123456,
            )
        )

    @dataclass
    class ConfDef7:
        start_url: str
        save_dir: str
        structure: list

    with pytest.raises(SchemaError):
        config = SiteConfig(
            ConfDef7(
                start_url="http://example.com",
                save_dir="/tmp",
                structure=[
                    {
                        "url": 1234567,
                    }
                ],
            )
        )

    @dataclass
    class ConfDef8:
        start_url: str
        save_dir: str
        structure: list

    with pytest.raises(SchemaError):
        config = SiteConfig(
            ConfDef8(
                start_url="http://example.com",
                save_dir="/tmp",
                structure=[
                    {
                        "url": r"http://example\.com/",
                        "file_path": 12345678,
                    }
                ],
            )
        )

    @dataclass
    class ConfDef9:
        start_url: str
        save_dir: str
        structure: list

    with pytest.raises(SchemaError):
        config = SiteConfig(
            ConfDef9(
                start_url="http://example.com",
                save_dir="/tmp",
                structure=[
                    {
                        "url": r"http://example\.com/",
                        "file_content": 12345678,
                    }
                ],
            )
        )

    @dataclass
    class ConfDef10:
        start_url: str
        save_dir: str
        structure: list

    with pytest.raises(SchemaError):
        config = SiteConfig(
            ConfDef10(
                start_url="http://example.com",
                save_dir="/tmp",
                structure=[
                    {
                        "url": r"http://example\.com/",
                        "assert": 12345678,
                    }
                ],
            )
        )


def test_site_config_create_by_file() -> None:
    SiteConfig.create_by_definition(path.join(site_config_dir, "site_config_000.py"))
    SiteConfig.create_by_definition(
        Path(site_config_dir).joinpath("site_config_000.py")
    )


def test_site_config_create_by_class() -> None:
    class ConfDef0:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [{"url": "http://example\.com/"}]

    SiteConfig.create_by_definition(ConfDef0)


def test_site_config_create_get_start_command() -> None:
    class ConfDef0:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [{"url": "http://example\.com/"}]

    config = SiteConfig(ConfDef0())
    command = config.get_start_command()
    assert isinstance(command, RequestUrlCommand)
    assert command.url == "http://example.com/"


def test_site_config_get_debug_environment() -> None:
    class ConfDef0:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": "http://(example\.com)/",
                "content": "//p[has-class('main')]",
            },
        ]

    config = SiteConfig(ConfDef0())

    command_candidates = config.get_simulated_command_candidates_for_url(
        "http://example.com/"
    )
    assert len(command_candidates) == 1
    command = command_candidates[0][1]
    assert isinstance(command, RequestUrlCommand)

    res_body = b"<p class='main foo'><a href='aaa.txt'>aaa</a></p><p class='bar'><a href='bbb.txt'>aaa</a></p>"
    res = fake_response(url=command.url, body=res_body)
    url_info = command.url_info
    debug_env = config.get_debug_environment(res, url_info)
    debug_env["explain"]()
    debug_env["help"]()
    assert debug_env["url"] == "http://example.com/"
    assert debug_env["url_match"].group(1) == "example.com"
    assert debug_env["content_node"].xpath("@class").get() == "main foo"
    print(debug_env["file_content"]())
    assert (
        debug_env["file_content"]()
        == b'<p class="main foo"><a href="aaa.txt">aaa</a></p>'
    )
    assert (
        debug_env["file_content_as_str"]()
        == '<p class="main foo"><a href="aaa.txt">aaa</a></p>'
    )
    debug_env["assert_content"]()
    assert debug_env["get_content_urls"]() == ["http://example.com/aaa.txt"]
    commands = debug_env["get_commands"]()
    assert len(commands) == 1
    command = commands[0]
    assert isinstance(command, SaveFileContentCommand)
    assert command.url == "http://example.com/"
    assert command.file_content == b'<p class="main foo"><a href="aaa.txt">aaa</a></p>'

    class ConfDef1:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": lambda url: url == "http://example.com/",
                "content": "//p",
                "file_content": lambda: b"invalid utf-8 \xf1",
                "assert": lambda: False,
            }
        ]

    config = SiteConfig(ConfDef1())

    command_candidates = config.get_simulated_command_candidates_for_url(
        "http://example.com/"
    )
    command = command_candidates[0][1]

    res_body = b"<p class='main foo'><a href='aaa.txt'>aaa</a></p><p class='bar'><a href='bbb.txt'>aaa</a></p>"
    res = fake_response(url=command.url, body=res_body)
    url_info = command.url_info
    debug_env = config.get_debug_environment(res, url_info)

    debug_env["explain"]()
    assert debug_env["url_match"] == None
    assert debug_env["content_node"].xpath("@class").getall() == ["main foo", "bar"]
    assert debug_env["get_content_urls"]() == [
        "http://example.com/aaa.txt",
        "http://example.com/bbb.txt",
    ]
    with pytest.raises(MediaScrapyError):
        debug_env["file_content_as_str"]()

    with pytest.raises(AssertionError):
        debug_env["assert_content"]()

    """
            "explain": lambda: print(
                "Happens in the next phase: \n"
                + "\n".join(
                    [
                        "    " + cmd.get_description()
                        for cmd in self.get_url_commands_impl(url_info, structure_node)
                    ]
                )
            ),
            "get_all_urls": lambda: get_links(url_info.res, url_info.content_node),
            # for debug
            "url_info": url_info,
            "structure_node": structure_node,
            "get_commands": lambda: self.get_url_commands_impl(
                url_info, structure_node
            ),
    """


def test_site_config_create_by_definition_error() -> None:
    with pytest.raises(MediaScrapyError):
        SiteConfig.create_by_definition(
            path.join(site_config_dir, "invalid_extension.txt")
        )

    with pytest.raises(MediaScrapyError):
        SiteConfig.create_by_definition(path.join(site_config_dir, "not_found.py"))

    try:
        syntax_error_site_config_path = Path(site_config_dir).joinpath(
            "syntax_error.py"
        )
        assert not syntax_error_site_config_path.exists()
        syntax_error_site_config_path.write_text("foo bar")

        assert syntax_error_site_config_path.exists()
        with pytest.raises(MediaScrapyError):
            SiteConfig.create_by_definition(syntax_error_site_config_path)
    finally:
        syntax_error_site_config_path.unlink()
    assert not syntax_error_site_config_path.exists()

    with pytest.raises(MediaScrapyError):
        SiteConfig.create_by_definition(
            path.join(site_config_dir, "site_config_no_class.py")
        )

    with pytest.raises(MediaScrapyError):
        SiteConfig.create_by_definition(
            path.join(site_config_dir, "site_config_duplicated.py")
        )


def test_get_url_commands_with_kwargs_url_matcher() -> None:
    class ConfDef:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": lambda **kwargs: kwargs["url"] == "http://example.com/",
                "file_path": "foo",
            },
            {
                "url": lambda **kwargs: kwargs["url"] == "http://example.com/aaa.txt",
                "file_path": "aaa.txt",
            },
        ]

    config = SiteConfig(ConfDef())
    res = fake_response(url="http://example.com/", body=b"<a href='aaa.txt'>aaa</a>")
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 1
    command = commands[0]
    assert isinstance(command, DownloadUrlCommand)
    assert command.file_path == "foo/aaa.txt"
    assert command.url == "http://example.com/aaa.txt"


def test_get_url_commands_with_file_content() -> None:
    class ConfDef:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "file_path": "test.json",
                "file_content": "//p/text()",
            }
        ]

    config = SiteConfig(ConfDef())
    res = fake_response(
        url="http://example.com/", body=b"<p>foo</p> <p>bar</p> <div>baz</div>"
    )
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 1
    command = commands[0]
    assert isinstance(command, SaveFileContentCommand)
    assert command.file_path == "test.json"
    assert command.file_content == json.dumps(["foo", "bar"]).encode("utf-8")
    assert isinstance(command.get_description(), str)

    class ConfDef2:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "file_path": "test.json",
                "file_content": "//p/text()",
            },
            {"url": r"http://example\.com/foo"},
        ]

    with pytest.raises(MediaScrapyError):
        SiteConfig(ConfDef2())


def test_get_url_commands_with_paging_file_path_obtained_before_request() -> None:
    def no_request_file_path_extractor(url_match: re.Match) -> str:
        page_number_str = url_match.group(2)
        if page_number_str is None:
            return "1"
        else:
            assert isinstance(page_number_str, str)
            return page_number_str

    class ConfDef1:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/(\?page=(\d+))?",
                "file_path": no_request_file_path_extractor,
                "paging": True,
            },
            {"url": r"http://example\.com/contents/(\w+)", "file_path": r"\g<1>.txt"},
        ]

    config = SiteConfig(ConfDef1())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/foo">foo</a><a href="/contents/bar">bar</a><a href="/?page=2">next</a>',
    )
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 3
    download_commands = [
        command for command in commands if isinstance(command, DownloadUrlCommand)
    ]
    assert len(download_commands) == 2
    assert download_commands[0].url == "http://example.com/contents/foo"
    assert download_commands[0].file_path == "1/foo.txt"
    assert isinstance(download_commands[0].get_description(), str)
    assert download_commands[1].url == "http://example.com/contents/bar"
    assert download_commands[1].file_path == "1/bar.txt"
    assert isinstance(download_commands[1].get_description(), str)

    request_url_commands = [
        command for command in commands if isinstance(command, RequestUrlCommand)
    ]
    assert len(request_url_commands) == 1
    request_url_command = request_url_commands[0]
    assert request_url_command.url_info.url == "http://example.com/?page=2"
    assert (
        request_url_command.url_info.link_el.extract() == '<a href="/?page=2">next</a>'
    )
    assert request_url_command.url_info.structure_path == [0]
    assert request_url_command.url_info.file_path == "2"
    assert isinstance(request_url_command.get_description(), str)

    res = fake_response(
        url="http://example.com/?page=2",
        body=b'<a href="/contents/aaa">aaa</a><a href="/contents/bbb">bbb</a><a href="/?page=3">next</a>',
        url_info=request_url_command.url_info,
    )
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 3
    download_commands = [
        command for command in commands if isinstance(command, DownloadUrlCommand)
    ]
    assert len(download_commands) == 2
    assert download_commands[0].url == "http://example.com/contents/aaa"
    assert download_commands[0].file_path == "2/aaa.txt"
    assert download_commands[1].url == "http://example.com/contents/bbb"
    assert download_commands[1].file_path == "2/bbb.txt"

    request_url_commands = [
        command for command in commands if isinstance(command, RequestUrlCommand)
    ]
    assert len(request_url_commands) == 1
    request_url_command = request_url_commands[0]
    assert request_url_command.url_info.url == "http://example.com/?page=3"
    assert (
        request_url_command.url_info.link_el.extract() == '<a href="/?page=3">next</a>'
    )
    assert request_url_command.url_info.structure_path == [0]
    assert request_url_command.url_info.file_path == "3"


def test_get_url_commands_with_paging_file_path_obtained_after_request() -> None:
    def with_request_file_path_extractor(res: Response, url_match: re.Match) -> str:
        page_number_str = url_match.group(2)
        if page_number_str is None:
            return "1"
        else:
            return "2"

    class ConfDef1:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/(\?page=(\d+))?",
                "file_path": with_request_file_path_extractor,
                "paging": True,
            },
            {"url": r"http://example\.com/contents/(\w+)", "file_path": r"\g<1>.txt"},
        ]

    config = SiteConfig(ConfDef1())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/foo">foo</a><a href="/contents/bar">bar</a><a href="/?page=2">next</a>',
    )
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 3
    download_commands = [
        command for command in commands if isinstance(command, DownloadUrlCommand)
    ]
    assert len(download_commands) == 2
    assert download_commands[0].url == "http://example.com/contents/foo"
    assert download_commands[0].file_path == "1/foo.txt"
    assert download_commands[1].url == "http://example.com/contents/bar"
    assert download_commands[1].file_path == "1/bar.txt"

    request_url_commands = [
        command for command in commands if isinstance(command, RequestUrlCommand)
    ]
    assert len(request_url_commands) == 1
    request_url_command = request_url_commands[0]
    assert request_url_command.url_info.url == "http://example.com/?page=2"
    assert (
        request_url_command.url_info.link_el.extract() == '<a href="/?page=2">next</a>'
    )
    assert request_url_command.url_info.structure_path == [0]
    assert request_url_command.url_info.file_path == ""

    res = fake_response(
        url="http://example.com/?page=2",
        body=b'<a href="/contents/aaa">aaa</a><a href="/contents/bbb">bbb</a><a href="/?page=3">next</a>',
        url_info=request_url_command.url_info,
    )
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 3
    download_commands = [
        command for command in commands if isinstance(command, DownloadUrlCommand)
    ]
    assert len(download_commands) == 2
    assert download_commands[0].url == "http://example.com/contents/aaa"
    assert download_commands[0].file_path == "2/aaa.txt"
    assert download_commands[1].url == "http://example.com/contents/bbb"
    assert download_commands[1].file_path == "2/bbb.txt"

    request_url_commands = [
        command for command in commands if isinstance(command, RequestUrlCommand)
    ]
    assert len(request_url_commands) == 1
    request_url_command = request_url_commands[0]
    assert request_url_command.url_info.url == "http://example.com/?page=3"
    assert (
        request_url_command.url_info.link_el.extract() == '<a href="/?page=3">next</a>'
    )
    assert request_url_command.url_info.structure_path == [0]
    assert request_url_command.url_info.file_path == ""


def test_get_url_commands_with_paging_without_url_match() -> None:
    def url_matcher(url: str) -> bool:
        parsed_url = urlparse(url)
        if parsed_url.query == "":
            is_query_ok = True
        else:
            parsed_query = parse_qsl(parsed_url.query)
            if (
                len(parsed_query) == 1
                and parsed_query[0][0] == "page"
                and re.fullmatch(r"\d+", parsed_query[0][1]) is not None
            ):
                is_query_ok = True
            else:
                is_query_ok = False
        return (
            parsed_url.scheme == "http"
            and parsed_url.hostname == "example.com"
            and parsed_url.port is None
            and parsed_url.path == "/"
            and is_query_ok
        )

    def file_path_extractor(url: str) -> str:
        parsed_url = urlparse(url)
        if parsed_url.query == "":
            return "1"
        parsed_query = parse_qs(parsed_url.query)
        return parsed_query["page"][0]

    class ConfDef1:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": url_matcher,
                "file_path": file_path_extractor,
                "paging": True,
            },
            {"url": r"http://example\.com/contents/(\w+)", "file_path": r"\g<1>.txt"},
        ]

    config = SiteConfig(ConfDef1())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/foo">foo</a><a href="/contents/bar">bar</a><a href="/?page=2">next</a>',
    )
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 3
    download_commands = [
        command for command in commands if isinstance(command, DownloadUrlCommand)
    ]
    assert len(download_commands) == 2
    assert download_commands[0].url == "http://example.com/contents/foo"
    assert download_commands[0].file_path == "1/foo.txt"
    assert download_commands[1].url == "http://example.com/contents/bar"
    assert download_commands[1].file_path == "1/bar.txt"

    request_url_commands = [
        command for command in commands if isinstance(command, RequestUrlCommand)
    ]
    assert len(request_url_commands) == 1
    request_url_command = request_url_commands[0]
    assert request_url_command.url_info.url == "http://example.com/?page=2"
    assert (
        request_url_command.url_info.link_el.extract() == '<a href="/?page=2">next</a>'
    )
    assert request_url_command.url_info.structure_path == [0]
    assert request_url_command.url_info.file_path == "2"

    res = fake_response(
        url="http://example.com/?page=2",
        body=b'<a href="/contents/aaa">aaa</a><a href="/contents/bbb">bbb</a><a href="/?page=3">next</a>',
        url_info=request_url_command.url_info,
    )
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 3
    download_commands = [
        command for command in commands if isinstance(command, DownloadUrlCommand)
    ]
    assert len(download_commands) == 2
    assert download_commands[0].url == "http://example.com/contents/aaa"
    assert download_commands[0].file_path == "2/aaa.txt"
    assert download_commands[1].url == "http://example.com/contents/bbb"
    assert download_commands[1].file_path == "2/bbb.txt"

    request_url_commands = [
        command for command in commands if isinstance(command, RequestUrlCommand)
    ]
    assert len(request_url_commands) == 1
    request_url_command = request_url_commands[0]
    assert request_url_command.url_info.url == "http://example.com/?page=3"
    assert (
        request_url_command.url_info.link_el.extract() == '<a href="/?page=3">next</a>'
    )
    assert request_url_command.url_info.structure_path == [0]
    assert request_url_command.url_info.file_path == "3"


def test_get_url_commands_without_url_match_object() -> None:
    class ConfDef1:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": lambda url: url == "http://example.com/",
                "file_path": "foo",
            },
            {
                "url": r"http://example\.com/contents/(\w+)",
                "file_path": r"\g<1>.txt",
            },
        ]

    config = SiteConfig(ConfDef1())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/foo">foo</a><a href="/contents/bar">bar</a>',
    )
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 2
    assert all(isinstance(command, DownloadUrlCommand) for command in commands)
    download_commands = cast(List[DownloadUrlCommand], commands)
    assert [command.url for command in download_commands] == [
        "http://example.com/contents/foo",
        "http://example.com/contents/bar",
    ]
    assert [command.file_path for command in download_commands] == [
        "foo/foo.txt",
        "foo/bar.txt",
    ]


def test_get_url_commands_multiple_root() -> None:
    class ConfDef1:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            [
                [
                    {
                        "url": lambda url: re.fullmatch(
                            r"http://example\.com/not_matched", url
                        ),
                        "file_path": r"foo",
                    },
                    {
                        "url": r"http://example\.com/not_matched/contents/(\w+)",
                        "file_path": r"\g<1>.txt",
                    },
                ],
                [
                    {
                        "url": lambda url: url == "http://example.com/",
                        "file_path": r"bar",
                    },
                    {
                        "url": r"http://example\.com/contents/(\w+)",
                        "file_path": r"\g<1>.txt",
                    },
                ],
            ]
        ]

    config = SiteConfig(ConfDef1())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/foo">foo</a><a href="/contents/bar">bar</a>',
    )
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 2
    assert all(isinstance(command, DownloadUrlCommand) for command in commands)
    download_commands = cast(List[DownloadUrlCommand], commands)
    assert [command.url for command in download_commands] == [
        "http://example.com/contents/foo",
        "http://example.com/contents/bar",
    ]
    assert [command.file_path for command in download_commands] == [
        "bar/foo.txt",
        "bar/bar.txt",
    ]


def test_get_url_commands_start_url_not_match_with_any_url_matcher() -> None:
    class ConfDef2:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            [
                [
                    {
                        "url": lambda url: url == "http://example.com/not_matched",
                        "file_path": r"foo",
                    },
                    {
                        "url": r"http://example\.com/not_matched/contents/(\w+)",
                        "file_path": r"\g<1>.txt",
                    },
                ],
                [
                    {
                        "url": lambda url: url == "http://example.com/not_matched_2",
                        "file_path": r"bar",
                    },
                    {
                        "url": r"http://example\.com/not_mached_2/contents/(\w+)",
                        "file_path": r"\g<1>.txt",
                    },
                ],
            ]
        ]

    config = SiteConfig(ConfDef2())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/foo">foo</a><a href="/contents/bar">bar</a>',
    )
    with pytest.raises(MediaScrapyError):
        config.get_url_commands(res, res.meta["url_info"])


def test_get_url_commands_using_as_url() -> None:
    class ConfDef1:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "file_path": r"foo",
            },
            {
                "url": r"http://example\.com/contents/(\w+)",
                "as_url": r"http://cdn.example.com/images/\g<1>.jpg",
                "file_path": r"\g<1>.jpg",
            },
        ]

    config = SiteConfig(ConfDef1())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/foo">foo</a><a href="/contents/bar">bar</a>',
    )
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 2
    assert all(isinstance(command, DownloadUrlCommand) for command in commands)
    download_commands = cast(List[DownloadUrlCommand], commands)
    assert [command.url for command in download_commands] == [
        "http://cdn.example.com/images/foo.jpg",
        "http://cdn.example.com/images/bar.jpg",
    ]
    assert [command.file_path for command in download_commands] == [
        "foo/foo.jpg",
        "foo/bar.jpg",
    ]

    class ConfDef2:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "file_path": r"foo",
            },
            {
                "url": lambda url: url.startswith("http://example.com/contents/"),
                "as_url": "http://cdn.example.com/images/aaa.jpg",
                "file_path": "bbb.jpg",
            },
        ]

    config = SiteConfig(ConfDef2())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/bar">bar</a>',
    )
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 1
    assert all(isinstance(command, DownloadUrlCommand) for command in commands)
    download_commands = cast(List[DownloadUrlCommand], commands)
    assert [command.url for command in download_commands] == [
        "http://cdn.example.com/images/aaa.jpg",
    ]
    assert [command.file_path for command in download_commands] == [
        "foo/bbb.jpg",
    ]

    class ConfDef3:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "file_path": r"foo",
            },
            {
                "url": r"http://example\.com/contents/(\w+)",
                "as_url": lambda url_match: f"http://cdn.example.com/images/{md5(url_match.group(1).encode('utf-8')).hexdigest()}.jpg",
                "file_path": r"\g<1>.jpg",
            },
        ]

    config = SiteConfig(ConfDef3())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/foo">foo</a><a href="/contents/bar">bar</a>',
    )
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 2
    assert all(isinstance(command, DownloadUrlCommand) for command in commands)
    download_commands = cast(List[DownloadUrlCommand], commands)
    assert [command.url for command in download_commands] == [
        f"http://cdn.example.com/images/{md5('foo'.encode('utf-8')).hexdigest()}.jpg",
        f"http://cdn.example.com/images/{md5('bar'.encode('utf-8')).hexdigest()}.jpg",
    ]
    assert [command.file_path for command in download_commands] == [
        "foo/foo.jpg",
        "foo/bar.jpg",
    ]

    class ConfDef4:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "file_path": r"foo",
            },
            {
                "url": r"http://example\.com/contents/(\w+)",
                "as_url": 123456789,
                "file_path": r"\g<1>.jpg",
            },
        ]

    with pytest.raises(SchemaError):
        SiteConfig(ConfDef4())

    class ConfDef5:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "file_path": r"foo",
            },
            {
                "url": r"http://example\.com/contents/(\w+)",
                "as_url": lambda content_node: f"http://example.com/{content_node.get()}/no_use_response_data",
                "file_path": r"\g<1>.jpg",
            },
        ]

    with pytest.raises(SchemaError):
        SiteConfig(ConfDef5())

    class ConfDef6:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "file_path": r"foo",
            },
            {
                "url": r"http://example\.com/contents/(\w+)",
                "as_url": lambda: None,
                "file_path": r"\g<1>.jpg",
            },
        ]

    config = SiteConfig(ConfDef6())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/foo">foo</a><a href="/contents/bar">bar</a>',
    )
    with pytest.raises(MediaScrapyError):
        config.get_url_commands(res, res.meta["url_info"])

    class ConfDef7:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
            },
            {
                "url": r"http://example\.com/contents/\w+",
                "as_url": r"http://cdn.example.com/images/\g<1>.jpg",
            },
        ]

    config = SiteConfig(ConfDef7())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/foo">foo</a><a href="/contents/bar">bar</a>',
    )

    with pytest.raises(MediaScrapyError):
        commands = config.get_url_commands(res, res.meta["url_info"])
        print(commands)


def test_get_url_commands_with_file_path() -> None:
    class ConfDef0:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {"url": r"http://example\.com/", "file_path": "foo"},
            {"url": r"http://example\.com/contents/(\w+)", "file_path": "\g<1>.jpg"},
        ]

    config = SiteConfig(ConfDef0())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/foo">foo</a><a href="/contents/bar">bar</a>',
    )
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 2
    assert all(isinstance(command, DownloadUrlCommand) for command in commands)
    download_commands = cast(List[DownloadUrlCommand], commands)
    assert [command.url for command in download_commands] == [
        "http://example.com/contents/foo",
        "http://example.com/contents/bar",
    ]
    assert [command.file_path for command in download_commands] == [
        "foo/foo.jpg",
        "foo/bar.jpg",
    ]

    class ConfDef1:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {"url": r"http://example\.com/", "file_path": "foo"},
            {"url": r"http://example\.com/contents/\w+", "file_path": "\g<1>.jpg"},
        ]

    config = SiteConfig(ConfDef1())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/foo">foo</a><a href="/contents/bar">bar</a>',
    )
    with pytest.raises(MediaScrapyError):
        config.get_url_commands(res, res.meta["url_info"])


def test_get_url_commands_specific_content_area() -> None:
    res = fake_response(
        url="http://example.com/",
        body=b"""
        <section class="navigation">
            <a href="/another_content_1">navigation_link_1</a>
        </section>
        <section class="main-content">
            <a href="/contents/foo.jpg">foo</a>
        </section>
        <section class="main-content">
            <a href="/contents/bar.jpg">bar</a>
        </section>
        <section class="navigation">
            <a href="/another_content_2">navigation_link_2</a>
        </section>
        """,
    )

    class ConfDef1:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "content": "//*[has-class('main-content')]",
                "file_path": r"foo",
            },
            {
                "url": r"http://example\.com/contents/(\w+\.jpg)",
                "file_path": r"\g<1>",
            },
        ]

    config = SiteConfig(ConfDef1())
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 2
    assert all(isinstance(command, DownloadUrlCommand) for command in commands)
    download_commands = cast(List[DownloadUrlCommand], commands)
    assert [command.url for command in download_commands] == [
        "http://example.com/contents/foo.jpg",
        "http://example.com/contents/bar.jpg",
    ]
    assert [command.file_path for command in download_commands] == [
        "foo/foo.jpg",
        "foo/bar.jpg",
    ]

    class ConfDef2:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "content": 123456789,
                "file_path": r"foo",
            },
            {
                "url": r"http://example\.com/contents/(\w+\.jpg)",
                "file_path": r"\g<1>",
            },
        ]

    with pytest.raises(SchemaError):
        SiteConfig(ConfDef2())

    class ConfDef3:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "content": lambda url, url_match, link_el, res: res.xpath(
                    "//*[has-class('main-content')]"
                ),
                "file_path": r"foo",
            },
            {
                "url": r"http://example\.com/contents/(\w+\.jpg)",
                "file_path": r"\g<1>",
            },
        ]

    config = SiteConfig(ConfDef3())
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 2
    assert all(isinstance(command, DownloadUrlCommand) for command in commands)
    download_commands = cast(List[DownloadUrlCommand], commands)
    assert [command.url for command in download_commands] == [
        "http://example.com/contents/foo.jpg",
        "http://example.com/contents/bar.jpg",
    ]
    assert [command.file_path for command in download_commands] == [
        "foo/foo.jpg",
        "foo/bar.jpg",
    ]

    class ConfDef4:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "content": lambda res, content_node: res.xpath(
                    "//*[has-class('main-content')]"
                ),
                "file_path": r"foo",
            },
            {
                "url": r"http://example\.com/contents/(\w+\.jpg)",
                "file_path": r"\g<1>",
            },
        ]

    with pytest.raises(SchemaError):
        SiteConfig(ConfDef4())


def test_get_url_commands_with_binary_file() -> None:
    class ConfDef1:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "file_path": r"foo",
            },
            {
                "url": r"http://example\.com/contents/(\w+)",
                "as_url": r"http://cdn.example.com/images/\g<1>.jpg",
                "file_path": r"\g<1>.jpg",
                "file_content": lambda url: url.encode("utf-32"),
            },
        ]

    config = SiteConfig(ConfDef1())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/foo">foo</a><a href="/contents/bar">bar</a>',
    )
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 2
    command = commands[0]
    assert isinstance(command, SaveFileContentCommand)
    assert command.file_path == "foo/foo.jpg"
    assert command.file_content == "http://cdn.example.com/images/foo.jpg".encode(
        "utf-32"
    )
    command = commands[1]
    assert isinstance(command, SaveFileContentCommand)
    assert command.file_path == "foo/bar.jpg"
    assert command.file_content == "http://cdn.example.com/images/bar.jpg".encode(
        "utf-32"
    )


def test_get_url_commands_assert_content() -> None:
    class ConfDef1:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "file_path": r"foo",
                "assert": "//a[.='bar']",
            },
            {
                "url": r"http://example\.com/contents/(\w+)",
                "file_path": r"\g<1>.jpg",
            },
        ]

    config = SiteConfig(ConfDef1())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/foo">foo</a><a href="/contents/bar">bar</a>',
    )
    commands = list(config.get_url_commands(res, res.meta["url_info"]))
    assert len(commands) == 2
    command = commands[0]
    assert isinstance(command, DownloadUrlCommand)
    command = commands[1]
    assert isinstance(command, DownloadUrlCommand)

    class ConfDef2:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "file_path": r"foo",
                "assert": "//a[.='baz']",
            },
            {
                "url": r"http://example\.com/contents/(\w+)",
                "file_path": r"\g<1>.jpg",
            },
        ]

    config = SiteConfig(ConfDef2())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/foo">foo</a><a href="/contents/bar">bar</a>',
    )
    with pytest.raises(AssertionError):
        config.get_url_commands(res, res.meta["url_info"])

    class ConfDef3:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "file_path": r"foo",
                "assert": [
                    "//a[.='foo']",
                    lambda res: 0 < len(res.xpath("//a[.='bar']")),
                ],
            },
            {
                "url": r"http://example\.com/contents/(\w+)",
                "file_path": r"\g<1>.jpg",
            },
        ]

    config = SiteConfig(ConfDef3())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/foo">foo</a><a href="/contents/bar">bar</a>',
    )
    config.get_url_commands(res, res.meta["url_info"])

    class ConfDef4:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "file_path": r"foo",
                "assert": lambda: False,
            },
            {
                "url": r"http://example\.com/contents/(\w+)",
                "file_path": r"\g<1>.jpg",
            },
        ]

    config = SiteConfig(ConfDef4())
    res = fake_response(
        url="http://example.com/",
        body=b'<a href="/contents/foo">foo</a><a href="/contents/bar">bar</a>',
    )
    with pytest.raises(AssertionError):
        config.get_url_commands(res, res.meta["url_info"])


def test_get_simulated_command_candidates_for_url() -> None:
    class ConfDef1:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
            },
            {
                "url": r"http://example\.com/aaa/(\w+)",
            },
            {
                "url": r"http://example\.com/aaa/(\w+)",  # duplicated
            },
            {"content": ".//a"},
        ]

    config = SiteConfig(ConfDef1())
    candidates = config.get_simulated_command_candidates_for_url("http://example.com/")
    assert len(candidates) == 1
    structure_desc, command = candidates[0]
    assert structure_desc == '{\n    "url": "http://example\\\\.com/",\n}\n'
    url_info = command.url_info
    assert url_info.url == "http://example.com/"
    assert url_info.link_el.attrib["href"] == "http://example.com/"
    assert isinstance(url_info.url_match, re.Match)
    assert url_info.url_match.group(0) == "http://example.com/"
    assert url_info.file_path == ""
    assert url_info.structure_path == [0]

    candidates = config.get_simulated_command_candidates_for_url(
        "http://example.com/aaa/bbb"
    )
    assert len(candidates) == 3

    candidates = config.get_simulated_command_candidates_for_url(
        "http://example.com/not_matched"
    )
    assert len(candidates) == 0


def test_structure_node_get_simulated_url_info_list() -> None:
    class ConfDef1:
        start_url = "http://example.com/"
        save_dir = "/tmp"
        structure = [
            {
                "url": r"http://example\.com/",
                "file_path": "foo",
            },
            {
                "url": r"http://example\.com/(\w+)",
                "file_path": lambda url_match: url_match.group(1),
            },
            {
                "url": r"http://example\.com/aaa/(\w+)",
                "file_path": r"bar-\g<1>",
            },
            [
                [
                    {
                        "url": r"http://example\.com/aaa/(\w+)",  # duplicated
                        "file_path": lambda: "baz",
                    },
                ],
                [
                    {
                        "url": r"http://example\.com/aaa/(\w+)",  # duplicated
                        "file_path": lambda res: "baa",
                    },
                ],
            ],
        ]

    config = SiteConfig(ConfDef1())
    url_info_list = config.root_structure_node.get_simulated_url_info_list(
        "http://example.com/"
    )
    assert len(url_info_list) == 1
    url_info = url_info_list[0]
    assert url_info.url == "http://example.com/"
    assert url_info.link_el.attrib["href"] == "http://example.com/"
    assert isinstance(url_info.url_match, re.Match)
    assert url_info.url_match.group(0) == "http://example.com/"
    assert url_info.file_path == "foo"
    assert url_info.structure_path == [0]

    url_info_list = config.root_structure_node.get_simulated_url_info_list(
        "http://example.com/aaa"
    )
    assert len(url_info_list) == 1
    url_info = url_info_list[0]
    assert url_info.url == "http://example.com/aaa"
    assert url_info.link_el.attrib["href"] == "http://example.com/aaa"
    assert isinstance(url_info.url_match, re.Match)
    assert url_info.url_match.group(0) == "http://example.com/aaa"
    assert url_info.file_path == "foo/aaa"
    assert url_info.structure_path == [0, 0]

    url_info_list = config.root_structure_node.get_simulated_url_info_list(
        "http://example.com/aaa/bbb"
    )
    assert len(url_info_list) == 3
    url_info = url_info_list[0]
    assert url_info.url == "http://example.com/aaa/bbb"
    assert url_info.link_el.attrib["href"] == "http://example.com/aaa/bbb"
    assert isinstance(url_info.url_match, re.Match)
    assert url_info.url_match.group(0) == "http://example.com/aaa/bbb"
    assert url_info.file_path == "foo/__unknown__/bar-bbb"
    assert url_info.structure_path == [0, 0, 0]
    url_info = url_info_list[1]
    assert url_info.url == "http://example.com/aaa/bbb"
    assert url_info.link_el.attrib["href"] == "http://example.com/aaa/bbb"
    assert isinstance(url_info.url_match, re.Match)
    assert url_info.url_match.group(0) == "http://example.com/aaa/bbb"
    assert url_info.file_path == r"foo/__unknown__/bar-\g<1>/baz"
    assert url_info.structure_path == [0, 0, 0, 0]
    url_info = url_info_list[2]
    assert url_info.url == "http://example.com/aaa/bbb"
    assert url_info.link_el.attrib["href"] == "http://example.com/aaa/bbb"
    assert isinstance(url_info.url_match, re.Match)
    assert url_info.url_match.group(0) == "http://example.com/aaa/bbb"
    assert url_info.file_path == r"foo/__unknown__/bar-\g<1>"
    assert url_info.structure_path == [0, 0, 0, 1]


def test_get_links() -> None:
    res = fake_response(
        url="http://example.com/",
        body=b"""
            <body>
                <a href="/aaa">aaa</a>
                <img src="/bbb">
                <foo href="/ccc"><bar src="/ddd"></foo>
            </body>
        """,
    )
    link_infos = get_links(res, res.xpath("//body"))
    assert len(link_infos) == 4
    assert all(isinstance(link_el, Selector) for link_el, url in link_infos)
    assert [url for link_el, url in link_infos] == [
        "http://example.com/aaa",
        "http://example.com/bbb",
        "http://example.com/ccc",
        "http://example.com/ddd",
    ]


def test_callable_component() -> None:
    fn = cast(Callable[..., str], lambda **kwargs: "foo")
    component = CallableComponent(source_obj=fn, fn=fn, can_accept_response=True)
    assert component(foo="foo", bar="bar") == "foo"
    assert (
        component.get_source_string()
        == 'fn = cast(Callable[..., str], lambda **kwargs: "foo")\n'
    )

    fn = cast(Callable[..., str], lambda res: "foo")
    component = CallableComponent(source_obj=fn, fn=fn, can_accept_response=True)
    assert component.needs_response
    component = CallableComponent(source_obj=fn, fn=fn, can_accept_response=False)
    assert not component.needs_response


def test_accepts_all_named_args() -> None:
    assert not accepts_all_named_args(lambda a: a)
    assert not accepts_all_named_args(lambda *args: args)
    assert not accepts_all_named_args(lambda a, *, b: a)
    assert accepts_all_named_args(lambda **kwargs: kwargs)


def test_get_all_required_named_args() -> None:
    assert get_all_required_named_args(lambda a: a) == ["a"]
    assert get_all_required_named_args(lambda b, a=None: a) == ["b"]
    assert get_all_required_named_args(lambda b, c, a=None: a) == ["b", "c"]


def test_get_source_string_for_obj() -> None:
    assert (
        re.fullmatch(
            r'\{ ("[abc]", ){3}\}', get_source_string_for_obj({"a", "b", "c"}, True)
        )
        is not None
    )
    assert get_source_string_for_obj(["a", "b", "c"], True) == '[ "a", "b", "c", ]'
    assert get_source_string_for_obj(("a", "b", "c"), True) == '( "a", "b", "c", )'
    assert (
        get_source_string_for_obj({"a": "foo", "b": "bar", "c": "baz"}, True)
        == '{ "a": "foo", "b": "bar", "c": "baz", }'
    )
    assert get_source_string_for_obj(set(), True) == "set()"
    assert get_source_string_for_obj(list(), True) == "[]"
    assert get_source_string_for_obj(tuple(), True) == "tuple()"
    assert get_source_string_for_obj(dict(), True) == "{}"

    class Foo:
        pass

    assert (
        re.fullmatch(
            r"<Foo object at 0x[0-9a-f]+>", get_source_string_for_obj(Foo(), True)
        )
        is not None
    )


def test_get_short_description_of_selector() -> None:
    assert (
        get_short_description_of_selector(
            Selector("<html><body><i>test</i></body></html>").xpath("//i")[0]
        )
        == "<i>test</i>"
    )
    assert (
        get_short_description_of_selector(
            Selector(
                "<html><body><ul class='list'><li>foo<li>bar<li>baz</ul></body></html>"
            ).xpath("//ul")[0]
        )
        == '<ul class="list"> ...'
    )
    assert (
        get_short_description_of_selector(
            Selector("<html><body><h1>title</h1></body></html>").xpath("//h1/text()")[0]
        )
        == "title"
    )
