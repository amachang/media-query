import inspect
from dataclasses import dataclass
from typing import Protocol, Optional, Callable, List, Any, Union
import re
import os
import functools
from urllib.parse import urldefrag
from collections import namedtuple
from schema import Schema, Or, SchemaError, Optional as SchemaOptional
from media_scrapy.errors import MediaScrapyError
from scrapy.http import Response


class SiteConfigDefinition(Protocol):
    start_url: str
    save_dir: str
    structure: list


class SiteConfig:
    root_structure_node: "RootStructureNode"

    def __init__(self, conf_def: SiteConfigDefinition):
        missing_attributes = list(
            filter(
                lambda attr: not hasattr(conf_def, attr),
                ["start_url", "save_dir", "structure"],
            )
        )
        if 0 < len(missing_attributes):
            raise MediaScrapyError(
                f"Site config doesn't have required attributes: {missing_attributes}"
            )

        self.save_dir = conf_def.save_dir
        os.makedirs(self.save_dir, exist_ok=True)

        self.start_url = conf_def.start_url

        if hasattr(conf_def, "login"):
            validate(conf_def.login, Schema({"url": str, "formdata": dict}))
            self.needs_login = True
            self.login = LoginConfig(conf_def.login["url"], conf_def.login["formdata"])
        else:
            self.needs_login = False

        self.root_structure_node = parse_structure(conf_def.structure)

        if hasattr(conf_def, "ignore_url"):
            validate(conf_def.ignore_url, UrlMatcherSchema())
            self.ignore_url_matcher = get_url_matcher(conf_def.ignore_url)
        else:
            self.ignore_url_matcher = lambda url: False

    def get_dirname(self, res: Response) -> Optional[str]:
        structure_path = res.meta["structure_path"]
        current_structure_node = self.root_structure_node.get_node_by_path(
            structure_path
        )
        assert isinstance(current_structure_node, DirStructureNode)
        dirname = current_structure_node.dirname_extractor(res)
        return dirname

    def get_url_infos(self, res: Response) -> List["SiteUrlInfo"]:
        urls = res.css("a::attr(href)").getall() + res.css("img::attr(src)").getall()
        urls = list(map(res.urljoin, urls))

        structure_path = res.meta["structure_path"]
        file_path = res.meta["file_path"]
        current_structure_node = self.root_structure_node.get_node_by_path(
            structure_path
        )
        assert not isinstance(current_structure_node, FileStructureNode)

        unknown_urls = []
        url_infos = []

        def strip_frament(url: str) -> str:
            return urldefrag(url)[0]

        urls = list(map(strip_frament, urls))

        url_seen = set()
        for url in urls:
            if url in url_seen:
                continue
            url_seen.add(url)

            if self.ignore_url_matcher(url):
                continue

            url_info: Optional[SiteUrlInfo] = None

            for index, child_structure_node in enumerate(
                current_structure_node.children
            ):
                if child_structure_node.url_matcher(url):
                    url_structure_path = structure_path + [index]
                    if isinstance(child_structure_node, NoDirStructureNode):
                        url_info = NoDirSiteUrlInfo(
                            url=url,
                            structure_path=url_structure_path,
                            file_path=file_path,
                        )
                    elif isinstance(child_structure_node, DirStructureNode):
                        url_info = DirSiteUrlInfo(
                            url=url,
                            structure_path=url_structure_path,
                            parent_file_path=file_path,
                        )
                    else:
                        assert isinstance(child_structure_node, FileStructureNode)
                        filename = child_structure_node.filename_extractor(url)
                        url_info = FileSiteUrlInfo(
                            url=url,
                            structure_path=url_structure_path,
                            file_path=file_path + [filename],
                        )
                    break

            if url_info is None:
                # check same depth page
                if (
                    isinstance(current_structure_node, BaseDirStructureNode)
                    and current_structure_node.paging
                    and current_structure_node.url_matcher(url)
                ):
                    if isinstance(current_structure_node, NoDirStructureNode):
                        url_info = NoDirSiteUrlInfo(
                            url=url,
                            structure_path=structure_path,
                            file_path=file_path,
                        )
                    else:
                        assert isinstance(current_structure_node, DirStructureNode)
                        url_info = DirSiteUrlInfo(
                            url=url,
                            structure_path=structure_path,
                            parent_file_path=file_path[:-1],
                        )
                else:
                    if not self.root_structure_node.match_any(url):
                        unknown_urls.append(url)
                    continue

            assert url_info is not None

            url_infos.append(url_info)

        if 0 < len(unknown_urls):
            raise MediaScrapyError(
                f"Unknown url found, please put these in ignore_url: {unknown_urls} in (url={res.url}, structure_path={structure_path}, file_path={file_path})"
            )

        # print(f'Url found {[info["url"] for info in url_infos]} in (url={res.url}, structure_path={structure_path}, file_path={file_path})')

        return url_infos


LoginConfig = namedtuple("LoginConfig", ["url", "formdata"])


@dataclass
class SiteUrlInfo:
    url: str
    structure_path: List[int]


@dataclass
class NoDirSiteUrlInfo(SiteUrlInfo):
    file_path: List[str]


@dataclass
class FileSiteUrlInfo(SiteUrlInfo):
    file_path: List[str]


@dataclass
class DirSiteUrlInfo(SiteUrlInfo):
    parent_file_path: List[str]


class StructureNode:
    children: List["NonRootStructureNode"]
    parent: Optional["StructureNode"]

    def __init__(self) -> None:
        self.children = []
        self.parent = None

    def add(self, node: "NonRootStructureNode") -> None:
        assert isinstance(node, StructureNode)
        assert node.parent is None
        node.parent = self
        self.children.append(node)

    def delete(self, node: "NonRootStructureNode") -> None:
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

    def match_any(self, url: str) -> bool:
        return any(matcher(url) for matcher in self.get_all_url_matchers())

    def get_all_url_matchers(self) -> List[Callable[[str], bool]]:
        url_matchers = []
        for child_node in self.children:
            child_url_matchers = child_node.get_all_url_matchers()
            url_matchers.extend(child_url_matchers)
        return url_matchers


class RootStructureNode(StructureNode):
    pass


class NonRootStructureNode(StructureNode):
    url_matcher: Callable[[str], bool]

    def __init__(self, url_matcher: Callable[[str], bool]) -> None:
        super().__init__()
        self.url_matcher = url_matcher

    def get_all_url_matchers(self) -> List[Callable[[str], bool]]:
        url_matchers = super().get_all_url_matchers()
        url_matchers.append(self.url_matcher)
        return url_matchers


class BaseDirStructureNode(NonRootStructureNode):
    paging: bool

    def __init__(
        self, url_matcher: Callable[[str], bool], paging: bool = False
    ) -> None:
        super().__init__(url_matcher)
        self.paging = paging


class NoDirStructureNode(BaseDirStructureNode):
    pass


class DirStructureNode(BaseDirStructureNode):
    dirname_extractor: Callable[[Response], Optional[str]]

    def __init__(
        self,
        url_matcher: Callable[[str], bool],
        dirname_extractor: Callable[[Response], Optional[str]],
        paging: bool = False,
    ) -> None:
        super().__init__(url_matcher, paging)
        self.dirname_extractor = dirname_extractor


class FileStructureNode(NonRootStructureNode):
    filename_extractor: Callable[[str], str]

    def __init__(
        self,
        url_matcher: Callable[[str], bool],
        filename_extractor: Callable[[str], str],
    ) -> None:
        super().__init__(url_matcher)
        self.filename_extractor = filename_extractor

    def add(self, node: NonRootStructureNode) -> None:
        assert False

    def delete(self, node: NonRootStructureNode) -> None:
        assert False


def parse_structure(structure_def: List[Any]) -> RootStructureNode:
    root_node = RootStructureNode()
    no_more_parent_node = False

    parent_node: StructureNode = root_node
    for structure_node_def in structure_def:
        if no_more_parent_node:
            raise MediaScrapyError(
                f"Unable to add new structure here: {structure_node_def}"
            )

        node: NonRootStructureNode
        if isinstance(structure_node_def, str):
            url_matcher = get_url_matcher(structure_node_def)
            node = NoDirStructureNode(url_matcher=url_matcher)
            parent_node.add(node)
            parent_node = node
        elif isinstance(structure_node_def, dict):
            validate(
                structure_node_def,
                Schema(
                    Or(
                        {"url": UrlMatcherSchema(), SchemaOptional("paging"): bool},
                        {
                            "dirname": DirnameExtractorSchema(),
                            "url": UrlMatcherSchema(),
                            SchemaOptional("paging"): bool,
                        },
                        {
                            "filename": FilenameExtractorSchema(),
                            "url": UrlMatcherSchema(),
                        },
                    )
                ),
            )
            url_matcher = get_url_matcher(structure_node_def["url"])
            if "dirname" in structure_node_def:
                dirname_extractor = get_dirname_extractor(structure_node_def["dirname"])
                paging = (
                    structure_node_def["paging"]
                    if "paging" in structure_node_def
                    else False
                )
                node = DirStructureNode(
                    url_matcher=url_matcher,
                    dirname_extractor=dirname_extractor,
                    paging=paging,
                )
            elif "filename" in structure_node_def:
                filename_extractor = get_filename_extractor(
                    structure_node_def["filename"]
                )
                node = FileStructureNode(
                    url_matcher=url_matcher,
                    filename_extractor=filename_extractor,
                )
                no_more_parent_node = True
            else:
                paging = (
                    structure_node_def["paging"]
                    if "paging" in structure_node_def
                    else False
                )
                node = NoDirStructureNode(url_matcher=url_matcher, paging=paging)

            parent_node.add(node)
            parent_node = node
        elif isinstance(structure_node_def, list):
            for sub_structure_def in structure_node_def:
                sub_root_node = parse_structure(sub_structure_def)
                assert len(sub_root_node.children) == 1
                node = sub_root_node.children[0]
                sub_root_node.delete(node)
                parent_node.add(node)
            no_more_parent_node = True
        else:
            raise MediaScrapyError(
                f"Invalid structure definition: {structure_node_def}"
            )

    return root_node


class FilenameExtractorSchema(Schema):
    def __init__(
        self, schema: Optional[Schema] = None, error: Optional[Exception] = None
    ) -> None:
        if schema is None:
            schema = object
        super().__init__(schema, error)

    def validate(
        self,
        filename_extractor_def: Union[str, Callable[[str], str]],
        _is_filename_extractor_schema: bool = True,
    ) -> Union[str, Callable[[str], str]]:
        data = super(FilenameExtractorSchema, self).validate(
            filename_extractor_def, _is_filename_extractor_schema=False
        )
        if _is_filename_extractor_schema:
            if isinstance(filename_extractor_def, str):
                try:
                    regex = re.compile(filename_extractor_def)
                except re.error as err:
                    raise SchemaError(
                        f"Invalid regular expression: {filename_extractor_def}"
                    ) from err
            elif callable(filename_extractor_def):
                pass
            else:
                assert False
        return filename_extractor_def


def get_filename_extractor(
    filename_extractor_def: Union[str, Callable[[str], str]]
) -> Callable[[str], str]:
    validate(filename_extractor_def, FilenameExtractorSchema())
    if isinstance(filename_extractor_def, str):
        regex = re.compile(filename_extractor_def)

        def filename_extractor(url: str) -> str:
            if regex.fullmatch(url) is None:
                raise MediaScrapyError(f"Invalid file url: {url} {regex}")
            return regex.sub(r"\g<1>", url)

        return filename_extractor
    elif callable(filename_extractor_def):
        return filename_extractor_def
    else:
        assert False


class DirnameExtractorSchema(Schema):
    def __init__(
        self, schema: Optional[Schema] = None, error: Optional[Exception] = None
    ) -> None:
        if schema is None:
            schema = object
        super().__init__(schema, error)

    def validate(
        self,
        dirname_extractor_def: Callable[[Response], str],
        _is_dirname_extractor_schema: bool = True,
    ) -> Callable[[Response], str]:
        data = super(DirnameExtractorSchema, self).validate(
            dirname_extractor_def, _is_dirname_extractor_schema=False
        )
        if _is_dirname_extractor_schema:
            if callable(dirname_extractor_def):
                pass
            else:
                assert False
        return dirname_extractor_def


def get_dirname_extractor(
    dirname_extractor_def: Callable[[Response], str]
) -> Callable[[Response], str]:
    validate(dirname_extractor_def, DirnameExtractorSchema())
    assert callable(dirname_extractor_def)
    return dirname_extractor_def


class UrlMatcherSchema(Schema):
    def __init__(
        self, schema: Optional[Schema] = None, error: Optional[Exception] = None
    ) -> None:
        if schema is None:
            schema = object
        super().__init__(schema, error)

    def validate(
        self,
        url_matcher_def: Union[str, Callable[[str], bool], List[Any]],
        _is_url_matcher_schema: bool = True,
    ) -> Union[str, Callable[[str], bool], List[Any]]:
        data = super(UrlMatcherSchema, self).validate(
            url_matcher_def, _is_url_matcher_schema=False
        )
        if _is_url_matcher_schema:
            if isinstance(url_matcher_def, str):
                try:
                    regex = re.compile(url_matcher_def)
                except re.error as err:
                    raise SchemaError(
                        f"Invalid regular expression: {url_matcher_def}"
                    ) from err
            elif isinstance(url_matcher_def, list):
                for sub_url_matcher_def in url_matcher_def:
                    self.validate(sub_url_matcher_def)
            elif callable(url_matcher_def):
                pass
            else:
                raise SchemaError(f"Unknown url matcher type: {url_matcher_def}")
        return url_matcher_def


def get_url_matcher(
    url_matcher_def: Union[str, Callable[[str], bool], List[Any]]
) -> Callable[[str], bool]:
    validate(url_matcher_def, UrlMatcherSchema())
    if isinstance(url_matcher_def, str):
        regex = re.compile(url_matcher_def)

        def url_matcher(url: str) -> bool:
            return regex.fullmatch(url) is not None

        return url_matcher
    elif isinstance(url_matcher_def, list):
        sub_url_matchers = []
        for sub_url_matcher_def in url_matcher_def:
            sub_url_matchers.append(get_url_matcher(sub_url_matcher_def))

        def url_matcher(url: str) -> bool:
            return any(matcher(url) for matcher in sub_url_matchers)

        return url_matcher
    elif callable(url_matcher_def):
        return url_matcher_def
    else:
        assert False


def validate(obj: Any, schema: Schema) -> None:
    try:
        schema.validate(obj)
    except SchemaError as err:
        raise MediaScrapyError(f"Site config schema error: {str(err)}") from err
