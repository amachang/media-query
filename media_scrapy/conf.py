import inspect
from dataclasses import dataclass
import json
from typing import (
    cast,
    runtime_checkable,
    Protocol,
    Optional,
    Callable,
    List,
    Any,
    Union,
    Dict,
    Set,
    TypeVar,
    Tuple,
    Generic,
)
from textwrap import indent
import re
import html
import os
from os import path
import functools
from urllib.parse import urldefrag
from collections import namedtuple
from media_scrapy.errors import MediaScrapyError
from scrapy.http import Response
from parsel import Selector, SelectorList, xpathfuncs
from schema import Schema, Or, SchemaError, Optional as SchemaOptional
from typeguard import typechecked
import personal_xpath_functions


@typechecked
@runtime_checkable
class SiteConfigDefinition(Protocol):
    start_url: str
    save_dir: str
    structure: list


@typechecked
class SiteConfig:
    root_structure_node: "StructureNode"

    def __init__(self, conf_def: SiteConfigDefinition):
        self.save_dir = Schema(str).validate(conf_def.save_dir)
        os.makedirs(self.save_dir, exist_ok=True)

        self.start_url = Schema(str).validate(conf_def.start_url)

        if hasattr(conf_def, "login"):
            login_def = Schema({"url": str, "formdata": dict}).validate(conf_def.login)
            self.needs_login = True
            self.login = LoginConfig(login_def["url"], login_def["formdata"])
        else:
            self.needs_login = False

        self.root_structure_node = parse_structure_list(conf_def.structure)

    def get_url_infos(self, res: Response) -> List["UrlInfo"]:
        if "url_info" not in res.meta:
            # first request
            return self.get_url_infos_with_parent_impl(
                res,
                parent_structure_path=[],
                parent_link_el=Selector(
                    f"<a href='{html.escape(res.url)}'>{res.xpath('//title/text()').get()}</a>"
                ),
                original_parent_file_path=".",
            )
        else:
            assert isinstance(res.meta["url_info"], ParseUrlInfo)
            return self.get_url_infos_with_parent_url_info(res, res.meta["url_info"])

    def get_url_infos_with_parent_url_info(
        self, res: Response, parent_url_info: "ParseUrlInfo"
    ) -> List["UrlInfo"]:
        return self.get_url_infos_with_parent_impl(
            res,
            parent_structure_path=parent_url_info.structure_path,
            parent_link_el=parent_url_info.link_el,
            parent_url_match=parent_url_info.url_match,
            original_parent_file_path=parent_url_info.file_path,
        )

    def get_url_infos_with_parent_impl(
        self,
        res: Response,
        parent_structure_path: List[int],
        original_parent_file_path: str,
        parent_link_el: Selector,
        parent_url_match: Optional[re.Match] = None,
    ) -> List["UrlInfo"]:
        parent_structure_node = self.root_structure_node.get_node_by_path(
            parent_structure_path
        )

        content_node = parent_structure_node.get_content_node(
            url=res.url,
            link_el=parent_link_el,
            url_match=parent_url_match,
            res=res,
        )
        parent_structure_node.assert_content(
            url=res.url,
            link_el=parent_link_el,
            url_match=parent_url_match,
            res=res,
            content_node=content_node,
        )
        parent_file_path_component = (
            parent_structure_node.get_file_path_component_after_request(
                url=res.url,
                link_el=parent_link_el,
                url_match=parent_url_match,
                res=res,
                content_node=content_node,
            )
        )
        parent_file_path = original_parent_file_path
        if parent_file_path_component is not None:
            parent_file_path = path.join(parent_file_path, parent_file_path_component)

        if parent_structure_node.is_leaf():
            if parent_structure_node.needs_response_for_file_content():
                file_content = parent_structure_node.extract_file_content(
                    url=res.url,
                    link_el=parent_link_el,
                    url_match=parent_url_match,
                    res=res,
                    content_node=content_node,
                )
            else:
                file_content = res.body

            return [
                FileContentUrlInfo(
                    url=res.url, file_path=parent_file_path, file_content=file_content
                )
            ]

        link_infos = None

        url_infos: List[UrlInfo] = []
        url_info: UrlInfo

        # search next page
        if parent_structure_node.paging:
            link_infos = (
                get_links(res, content_node) if link_infos is None else link_infos
            )
            for link_el, url in link_infos:
                is_url_matched, url_match = parent_structure_node.match_url(url)
                if is_url_matched:
                    assert not parent_structure_node.is_leaf()
                    converted_url = parent_structure_node.convert_url(
                        url=url, link_el=link_el, url_match=url_match
                    )
                    next_page_file_path = original_parent_file_path
                    if parent_structure_node.can_get_file_path_before_request():
                        next_page_file_path = path.dirname(next_page_file_path)
                        file_path_component = parent_structure_node.get_file_path_component_before_request(
                            url=converted_url, link_el=link_el, url_match=url_match
                        )
                        assert isinstance(file_path_component, str)
                        next_page_file_path = path.join(
                            next_page_file_path, file_path_component
                        )
                    url_info = ParseUrlInfo(
                        url=converted_url,
                        file_path=next_page_file_path,
                        structure_path=parent_structure_path,
                        link_el=link_el,
                        url_match=url_match,
                    )
                    url_infos.append(url_info)

        forwardable_structure_node_found = False

        for structure_index, structure_node in enumerate(
            parent_structure_node.children
        ):
            if structure_node.needs_no_request() or parent_structure_node.is_root:
                if parent_structure_node.is_root:
                    parent_url_is_matched, parent_url_match = structure_node.match_url(
                        res.url
                    )
                    if not parent_url_is_matched:
                        continue

                forwardable_structure_node_found = True

                file_path_component = (
                    structure_node.get_file_path_component_before_request(
                        url=res.url, link_el=parent_link_el, url_match=parent_url_match
                    )
                )
                file_path = parent_file_path
                if file_path_component is not None:
                    file_path = path.join(file_path, file_path_component)

                converted_url = structure_node.convert_url(
                    url=res.url,
                    link_el=parent_link_el,
                    url_match=parent_url_match,
                )

                url_info = ParseUrlInfo(
                    url=converted_url,
                    file_path=file_path,
                    structure_path=parent_structure_path + [structure_index],
                    link_el=parent_link_el,
                    url_match=parent_url_match,
                )
                sub_url_infos = self.get_url_infos_with_parent_url_info(res, url_info)
                url_infos.extend(sub_url_infos)
            else:
                link_infos = (
                    get_links(res, content_node) if link_infos is None else link_infos
                )
                for link_el, url in link_infos:
                    is_url_matched, url_match = structure_node.match_url(url)
                    if is_url_matched:
                        file_path_component = (
                            structure_node.get_file_path_component_before_request(
                                url=url, link_el=link_el, url_match=url_match
                            )
                        )
                        file_path = parent_file_path
                        if file_path_component is not None:
                            file_path = path.join(file_path, file_path_component)

                        converted_url = structure_node.convert_url(
                            url=url, link_el=link_el, url_match=url_match
                        )
                        needs_response_for_file = (
                            structure_node.needs_response_for_file_path()
                            or structure_node.needs_response_for_file_content()
                        )

                        if structure_node.is_leaf() and not needs_response_for_file:
                            if structure_node.can_get_file_content_before_request():
                                file_content = structure_node.extract_file_content_without_response(
                                    url=converted_url,
                                    link_el=link_el,
                                    url_match=url_match,
                                )
                                url_info = FileContentUrlInfo(
                                    url=converted_url,
                                    file_path=file_path,
                                    file_content=file_content,
                                )
                            else:
                                url_info = DownloadUrlInfo(
                                    url=converted_url,
                                    file_path=file_path,
                                )
                        else:
                            structure_path = parent_structure_path + [structure_index]
                            url_info = ParseUrlInfo(
                                url=converted_url,
                                file_path=file_path,
                                structure_path=structure_path,
                                link_el=link_el,
                                url_match=url_match,
                            )
                        url_infos.append(url_info)

        if not forwardable_structure_node_found and parent_structure_node.is_root:
            url_matcher_sources = [
                f"{index}: <no url matcher in definition>\n"
                if node.url_matcher is None
                else f"{index}: {node.url_matcher.get_source_string()}"
                for index, node in enumerate(parent_structure_node.children)
            ]
            url_matcher_sources_text = "".join(url_matcher_sources)
            raise MediaScrapyError(
                f"Start url doesn't much any url matcher: \n{indent(url_matcher_sources_text, '    ')}"
            )

        return url_infos


LoginConfig = namedtuple("LoginConfig", ["url", "formdata"])


@typechecked
@dataclass
class UrlInfo:
    url: str
    file_path: str


@typechecked
@dataclass
class DownloadUrlInfo(UrlInfo):
    pass


@typechecked
@dataclass
class FileContentUrlInfo(UrlInfo):
    file_content: bytes


@typechecked
@dataclass
class ParseUrlInfo(UrlInfo):
    link_el: Selector
    structure_path: List[int]
    url_match: Optional[re.Match]


U = TypeVar("U")


@typechecked
class CallableSiteConfigComponent(Generic[U]):
    source_obj: Any
    fn: Callable[..., Optional[U]]
    accepts_all_named_args: bool
    acceptable_named_args: List[str]
    needs_response: bool

    def __init__(
        self,
        source_obj: Any,
        fn: Callable[..., Optional[U]],
        can_accept_response: bool,
    ) -> None:
        self.source_obj = source_obj
        self.fn = fn
        self.accepts_all_named_args = accepts_all_named_args(self.fn)
        self.acceptable_named_args = get_all_acceptable_named_args(self.fn)
        self.needs_response = can_accept_response and any(
            arg in self.acceptable_named_args for arg in ["res", "content_node"]
        )

    def __call__(self, *args: Any, **kwargs: Any) -> U:
        result: Optional[U]
        if self.accepts_all_named_args:
            result = self.fn(**kwargs)
        else:
            acceptable_kwargs = {
                k: v for k, v in kwargs.items() if k in self.acceptable_named_args
            }
            result = self.fn(**acceptable_kwargs)
        if result is None:
            raise MediaScrapyError(
                f"Return None value from site config component below: \n{indent(self.get_source_string(), '    ')}\n"
            )
        return result

    def get_source_string(self) -> str:
        try:
            source_string = f"{inspect.getsource(self.source_obj)}"
        except:
            source_string = f"{self.source_obj}\n"

        assert re.search(r"\n$", source_string)
        return source_string


@typechecked
class StructureNode:
    children: List["StructureNode"]
    parent: Optional["StructureNode"]
    source_obj: Any
    url_matcher: Optional[CallableSiteConfigComponent[Union[bool, re.Match]]]
    url_converter: Optional[CallableSiteConfigComponent[str]]
    content_node_extractor: Optional[CallableSiteConfigComponent[SelectorList]]
    file_content_extractor: Optional[CallableSiteConfigComponent[Union[str, bytes]]]
    file_path_extractor: Optional[CallableSiteConfigComponent[str]]
    assertion_matcher: Optional[CallableSiteConfigComponent[None]]
    paging: bool
    is_root: bool

    def __init__(
        self,
        source_obj: Any,
        url_matcher: Optional[
            CallableSiteConfigComponent[Union[bool, re.Match]]
        ] = None,
        url_converter: Optional[CallableSiteConfigComponent[str]] = None,
        content_node_extractor: Optional[
            CallableSiteConfigComponent[SelectorList]
        ] = None,
        file_content_extractor: Optional[
            CallableSiteConfigComponent[Union[str, bytes]]
        ] = None,
        file_path_extractor: Optional[CallableSiteConfigComponent[str]] = None,
        assertion_matcher: Optional[CallableSiteConfigComponent[None]] = None,
        paging: bool = False,
        is_root: bool = False,
    ) -> None:
        self.children = []
        self.parent = None
        self.source_obj = source_obj
        self.url_matcher = url_matcher
        self.url_converter = url_converter
        self.content_node_extractor = content_node_extractor
        self.file_content_extractor = file_content_extractor
        self.file_path_extractor = file_path_extractor
        self.assertion_matcher = assertion_matcher
        self.paging = paging
        self.is_root = is_root

    def needs_no_request(self) -> bool:
        return self.url_matcher is None

    def is_leaf(self) -> bool:
        return len(self.children) == 0

    def needs_response_for_file_path(self) -> bool:
        if self.file_path_extractor is None:
            return False
        else:
            return self.file_path_extractor.needs_response

    def can_get_file_path_before_request(self) -> bool:
        if self.file_path_extractor is None:
            return False
        else:
            return not self.file_path_extractor.needs_response

    def needs_response_for_file_content(self) -> bool:
        if self.file_content_extractor is None:
            return False
        else:
            return self.file_content_extractor.needs_response

    def can_get_file_content_before_request(self) -> bool:
        if self.file_content_extractor is None:
            return False
        else:
            return not self.file_content_extractor.needs_response

    def add(self, node: "StructureNode") -> None:
        assert isinstance(node, StructureNode)
        assert node.parent is None
        node.parent = self
        self.children.append(node)

    def delete(self, node: "StructureNode") -> None:
        assert self == node.parent
        index = self.children.index(node)
        self.children = self.children[:index] + self.children[index + 1 :]
        node.parent = None

    def get_node_by_path(self, path: List[int]) -> "StructureNode":
        assert isinstance(path, list)
        if len(path) == 0:
            return self
        else:
            child_index = path[0]
            assert child_index < len(self.children)
            child_node = self.children[child_index]
            return child_node.get_node_by_path(path[1:])

    def match_url(self, url: str) -> Tuple[bool, Optional[re.Match]]:
        if self.url_matcher is None:
            return False, None
        else:
            matched = self.url_matcher(url=url)
            if isinstance(matched, bool):
                return matched, None
            else:
                assert isinstance(matched, re.Match)
                return True, matched

    def convert_url(
        self,
        url: str,
        link_el: Selector,
        url_match: Optional[re.Match],
    ) -> str:
        if self.url_converter is not None:
            converted_url = self.url_converter(
                url=url, link_el=link_el, url_match=url_match
            )
            return converted_url
        else:
            return url

    def get_content_node(
        self,
        url: str,
        link_el: Selector,
        url_match: Optional[re.Match],
        res: Response,
    ) -> SelectorList:
        if self.content_node_extractor:
            return self.content_node_extractor(
                url=url, link_el=link_el, url_match=url_match, res=res
            )
        else:
            return SelectorList([res.selector])

    def get_file_path_component_before_request(
        self,
        url: str,
        link_el: Selector,
        url_match: Optional[re.Match],
    ) -> Optional[str]:
        if (
            self.file_path_extractor is not None
            and not self.needs_response_for_file_path()
        ):
            result = self.file_path_extractor(
                url=url, link_el=link_el, url_match=url_match
            )
            assert isinstance(result, str)
            return result
        else:
            return None

    def get_file_path_component_after_request(
        self,
        url: str,
        res: Response,
        content_node: SelectorList,
        link_el: Selector,
        url_match: Optional[re.Match],
    ) -> Optional[str]:
        if self.needs_response_for_file_path():
            assert self.file_path_extractor is not None
            result = self.file_path_extractor(
                url=url,
                res=res,
                content_node=content_node,
                link_el=link_el,
                url_match=url_match,
            )
            assert isinstance(result, str)
            return result
        else:
            return None

    def extract_file_content(
        self,
        url: str,
        res: Response,
        content_node: SelectorList,
        link_el: Selector,
        url_match: Optional[re.Match],
    ) -> bytes:
        assert self.file_content_extractor is not None
        return self.extract_file_content_impl(
            {
                "url": url,
                "res": res,
                "content_node": content_node,
                "link_el": link_el,
                "url_match": url_match,
            }
        )

    def extract_file_content_without_response(
        self,
        url: str,
        link_el: Selector,
        url_match: Optional[re.Match],
    ) -> bytes:
        assert self.file_content_extractor is not None
        return self.extract_file_content_impl(
            {
                "url": url,
                "link_el": link_el,
                "url_match": url_match,
            }
        )

    def extract_file_content_impl(self, kwargs: Dict[str, Any]) -> bytes:
        assert self.file_content_extractor is not None
        file_content = self.file_content_extractor(**kwargs)
        if isinstance(file_content, str):
            return file_content.encode("utf-8")
        else:
            assert isinstance(file_content, bytes)
            return file_content

    def assert_content(
        self,
        url: str,
        res: Response,
        content_node: SelectorList,
        link_el: Optional[Selector],
        url_match: Optional[re.Match],
    ) -> None:
        if self.assertion_matcher is not None:
            kwargs = {
                "url": url,
                "res": res,
                "content_node": content_node,
                "link_el": link_el,
                "url_match": url_match,
            }
            self.assertion_matcher(
                url=url,
                res=res,
                content_node=content_node,
                link_el=link_el,
                url_match=url_match,
            )

    def check(self) -> None:
        if not self.is_leaf() and self.file_content_extractor is not None:
            raise MediaScrapyError(
                f"file_content can be only in last definition: \n{indent(self.file_content_extractor.get_source_string(), '    ')}"
            )

        for child_node in self.children:
            child_node.check()


def get_links(res: Response, content_node: SelectorList) -> List[Tuple[Selector, str]]:
    results = []
    for link_el in content_node.xpath(".//*[@href | @src]"):
        node_name = link_el.xpath("name(.)").get()
        if node_name in {"a", "area", "link"} and "href" in link_el.attrib:
            url = link_el.attrib["href"]
        elif (
            node_name
            in {
                "img",
                "embed",
                "iframe",
                "img",
                "input",
                "script",
                "source",
                "track",
                "video",
            }
            and "src" in link_el.attrib
        ):
            url = link_el.attrib["src"]
        elif "href" in link_el.attrib:
            url = link_el.attrib["href"]
        elif "src" in link_el.attrib:
            url = link_el.attrib["src"]
        else:
            assert False
        url = res.urljoin(url)
        results.append((link_el, url))
    return results


@typechecked
def parse_structure_list(
    structure_node_def_list: List[Union[List, Dict, str]]
) -> StructureNode:
    root_node = StructureNode(source_obj=None, is_root=True)
    after_branch_node = False

    parent_node = root_node
    for structure_node_def in structure_node_def_list:
        if after_branch_node:
            raise MediaScrapyError(
                f"Once branched structure nodes cannot be merged in a single node: \n{indent(str(structure_node_def), '    ')}"
            )

        if isinstance(structure_node_def, dict) or isinstance(structure_node_def, str):
            node = parse_structure(structure_node_def)
            parent_node.add(node)
            parent_node = node
        elif isinstance(structure_node_def, list):
            for sub_structure_node_def_list in structure_node_def:
                sub_root_node = parse_structure_list(sub_structure_node_def_list)
                assert sub_root_node.is_root
                for sub_node in sub_root_node.children:
                    assert not sub_node.is_root
                    sub_root_node.delete(sub_node)
                    parent_node.add(sub_node)
            after_branch_node = True
        else:
            raise MediaScrapyError(
                f"Invalid structure definition: \n{indent(str(structure_node_def), '    ')}"
            )

    root_node.check()

    return root_node


@typechecked
def parse_structure(structure_node_def: Union[Dict, str]) -> StructureNode:
    if isinstance(structure_node_def, str):
        url_matcher = UrlMatcherSchema().validate(structure_node_def)
        return StructureNode(source_obj=structure_node_def, url_matcher=url_matcher)
    else:
        structure_node_parsed = Schema(
            {
                SchemaOptional("url", default=None): UrlMatcherSchema(),
                SchemaOptional("as_url", default=None): UrlConverterSchema(),
                SchemaOptional("content", default=None): ContentNodeExtractorSchema(),
                SchemaOptional("file_content", default=None): ContentExtractorSchema(),
                SchemaOptional("file_path", default=None): FilePathExtractorSchema(),
                SchemaOptional("assert", default=None): AssertionMatcherSchema(),
                SchemaOptional("paging", default=False): bool,
            },
        ).validate(structure_node_def)

        return StructureNode(
            source_obj=structure_node_def,
            url_matcher=structure_node_parsed["url"],
            url_converter=structure_node_parsed["as_url"],
            content_node_extractor=structure_node_parsed["content"],
            file_content_extractor=structure_node_parsed["file_content"],
            file_path_extractor=structure_node_parsed["file_path"],
            assertion_matcher=structure_node_parsed["assert"],
            paging=structure_node_parsed["paging"],
        )


@typechecked
class RegexSchema(Schema):
    def __init__(self) -> None:
        super().__init__(None)

    def validate(self, regex_def: Union[str, re.Pattern]) -> re.Pattern:
        try:
            regex = re.compile(regex_def)
        except re.error as err:
            raise SchemaError(f"Invalid regular expression: {regex_def}") from err
        return regex


@typechecked
class UrlMatcherSchema(Schema):
    def __init__(self) -> None:
        super().__init__(None)
        self.regex_schema = RegexSchema()

    def validate(
        self,
        url_matcher_def: Union[
            str,
            Callable[..., Union[bool, re.Match, None]],
        ],
    ) -> CallableSiteConfigComponent[Union[bool, re.Match]]:
        if isinstance(url_matcher_def, str) or isinstance(url_matcher_def, re.Pattern):
            regex = self.regex_schema.validate(url_matcher_def)

            def url_matcher(url: str) -> Union[bool, re.Match]:
                url_match = regex.fullmatch(url)
                if url_match is None:
                    return False
                else:
                    return url_match

            return CallableSiteConfigComponent(
                source_obj=url_matcher_def, fn=url_matcher, can_accept_response=False
            )

        elif callable(url_matcher_def):
            check_supports_named_args_for_function(url_matcher_def, {"url"})
            url_matcher_impl = url_matcher_def

            def url_matcher(url: str) -> Union[bool, re.Match]:
                result = url_matcher_impl(url)
                if result is None:
                    return False
                else:
                    return result

            return CallableSiteConfigComponent(
                source_obj=url_matcher_def, fn=url_matcher, can_accept_response=False
            )

        else:
            raise SchemaError(f"Unknown url matcher type: {url_matcher_def}")


@typechecked
class UrlConverterSchema(Schema):
    def __init__(self) -> None:
        super().__init__(None)

    def validate(
        self, definition: Union[str, Callable[..., Optional[str]]]
    ) -> CallableSiteConfigComponent[str]:
        if isinstance(definition, str):
            match_expansion_template = definition

            def url_converter(url_match: Optional[re.Match]) -> str:
                if url_match is None:
                    return match_expansion_template
                else:
                    return url_match.expand(match_expansion_template)

            return CallableSiteConfigComponent(
                source_obj=definition, fn=url_converter, can_accept_response=False
            )

        assert callable(definition)

        check_supports_named_args_for_function(
            definition, {"url", "link_el", "url_match"}
        )

        return CallableSiteConfigComponent(
            source_obj=definition, fn=definition, can_accept_response=False
        )


@typechecked
class ContentNodeExtractorSchema(Schema):
    def __init__(self) -> None:
        super().__init__(None)

    def validate(
        self, definition: Union[str, Callable[..., SelectorList]]
    ) -> CallableSiteConfigComponent[SelectorList]:
        if isinstance(definition, str):
            xpath = definition

            def content_node_extractor(res: Response) -> SelectorList:
                return cast(SelectorList, res.xpath(xpath))

            return CallableSiteConfigComponent(
                source_obj=definition,
                fn=content_node_extractor,
                can_accept_response=True,
            )

        assert callable(definition)

        check_supports_named_args_for_function(
            definition, {"url", "link_el", "url_match", "res"}
        )

        return CallableSiteConfigComponent(
            source_obj=definition, fn=definition, can_accept_response=True
        )


@typechecked
class FilePathExtractorSchema(Schema):
    def __init__(self) -> None:
        super().__init__(None)

    def validate(
        self, definition: Union[str, Callable[..., Optional[str]]]
    ) -> CallableSiteConfigComponent[str]:
        if isinstance(definition, str):
            match_expansion_template = definition

            def file_path_extractor(url_match: Optional[re.Match]) -> str:
                if url_match is None:
                    return match_expansion_template
                else:
                    return url_match.expand(match_expansion_template)

            return CallableSiteConfigComponent(
                source_obj=definition, fn=file_path_extractor, can_accept_response=True
            )

        assert callable(definition)

        check_supports_named_args_for_function(
            definition, {"url", "link_el", "url_match", "res", "content_node"}
        )

        return CallableSiteConfigComponent(
            source_obj=definition, fn=definition, can_accept_response=True
        )


@typechecked
class ContentExtractorSchema(Schema):
    def __init__(self) -> None:
        super().__init__(None)

    def validate(
        self, definition: Union[str, Callable[..., Union[str, bytes, None]]]
    ) -> CallableSiteConfigComponent[Union[str, bytes]]:
        if isinstance(definition, str):
            xpath = definition

            def content_extractor(content_node: SelectorList) -> str:
                content = content_node.xpath(xpath).getall()
                return json.dumps(content)

            return CallableSiteConfigComponent(
                source_obj=definition, fn=content_extractor, can_accept_response=True
            )

        assert callable(definition)

        check_supports_named_args_for_function(
            definition, {"url", "link_el", "url_match", "res", "content_node"}
        )

        return CallableSiteConfigComponent(
            source_obj=definition, fn=definition, can_accept_response=True
        )


@typechecked
class AssertionMatcherSchema(Schema):
    def __init__(self) -> None:
        super().__init__(None)

    def validate(
        self, assertion_matcher_def: Union[str, Callable[..., bool], List]
    ) -> CallableSiteConfigComponent[bool]:
        if isinstance(assertion_matcher_def, list):
            sub_matchers = []
            for sub_assertion_matcher_def in assertion_matcher_def:
                sub_matcher = self.validate(sub_assertion_matcher_def)
                sub_matchers.append(sub_matcher)

            def multiple_assertion_matcher(
                url: str,
                link_el: Selector,
                url_match: re.Match,
                res: Response,
                content_node: SelectorList,
            ) -> bool:
                for sub_matcher in sub_matchers:
                    sub_matcher(
                        url=url,
                        link_el=link_el,
                        url_match=url_match,
                        res=res,
                        content_node=content_node,
                    )
                return True

            return CallableSiteConfigComponent(
                source_obj=assertion_matcher_def,
                fn=multiple_assertion_matcher,
                can_accept_response=True,
            )

        if isinstance(assertion_matcher_def, str):
            xpath = assertion_matcher_def

            def xpath_assertion_matcher(content_node: SelectorList) -> bool:
                if content_node.xpath(f"boolean({xpath})").get() == "0":
                    raise AssertionError(f"AssertionMatcher failed xpath: {xpath}")
                return True

            return CallableSiteConfigComponent(
                source_obj=xpath,
                fn=xpath_assertion_matcher,
                can_accept_response=True,
            )

        assert callable(assertion_matcher_def)

        check_supports_named_args_for_function(
            assertion_matcher_def,
            {"url", "link_el", "url_match", "res", "content_node"},
        )

        assertion_matcher_impl = assertion_matcher_def
        assertion_matcher_sub_component = CallableSiteConfigComponent(
            source_obj=assertion_matcher_impl,
            fn=assertion_matcher_impl,
            can_accept_response=True,
        )

        def assertion_matcher(
            url: str,
            link_el: Selector,
            url_match: re.Match,
            res: Response,
            content_node: SelectorList,
        ) -> bool:
            if not assertion_matcher_sub_component(
                url=url,
                link_el=link_el,
                url_match=url_match,
                res=res,
                content_node=content_node,
            ):
                matcher_source = f"\n{inspect.getsource(assertion_matcher_impl)}\n"
                raise AssertionError(
                    f"AssertionMatcher failed in function: {matcher_source}"
                )
            return True

        return CallableSiteConfigComponent(
            source_obj=assertion_matcher_impl,
            fn=assertion_matcher,
            can_accept_response=True,
        )


T = TypeVar("T")


@typechecked
def check_supports_named_args_for_function(
    extractor_fn: Callable, to_be_passed_named_args: Set[str]
) -> None:
    if not accepts_all_named_args(extractor_fn):
        required_named_args = get_all_required_named_args(extractor_fn)
        not_to_be_passed_named_args = list(
            filter(lambda arg: arg not in to_be_passed_named_args, required_named_args)
        )
        if 0 < len(not_to_be_passed_named_args):
            raise SchemaError(
                f"Unsupported argument names detected in function below: {', '.join(not_to_be_passed_named_args)}\n{indent(inspect.getsource(extractor_fn), '    ')}"
            )


@typechecked
def accepts_all_named_args(fn: Callable) -> bool:
    signature = inspect.signature(fn)
    return any(p.kind == p.VAR_KEYWORD for p in signature.parameters.values())


@typechecked
def get_all_required_named_args(fn: Callable) -> List[str]:
    parameters = get_named_parameter_objs(fn)
    parameters = list(filter(lambda p: p.default == p.empty, parameters))
    parameter_names = [p.name for p in parameters]
    return parameter_names


@typechecked
def get_all_acceptable_named_args(fn: Callable) -> List[str]:
    parameters = get_named_parameter_objs(fn)
    parameter_names = [p.name for p in parameters]
    return parameter_names


@typechecked
def get_named_parameter_objs(fn: Callable) -> List[inspect.Parameter]:
    signature = inspect.signature(fn)
    return list(
        filter(
            lambda p: p.kind in {p.KEYWORD_ONLY, p.POSITIONAL_OR_KEYWORD},
            signature.parameters.values(),
        )
    )
