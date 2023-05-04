from pathlib import Path
from scrapy.settings import Settings
from scrapy.crawler import CrawlerRunner
from media_scrapy import settings as setting_definitions
from media_scrapy.spiders import MainSpider, DebugSpider
from twisted.python.failure import Failure
from scrapy.utils.log import configure_logging
from typing import Union, Type, Any, Optional, List, Dict, cast
import traceback
from typeguard import typechecked
from twisted.internet.defer import Deferred
from twisted.internet.error import ReactorNotRunning
from media_scrapy.conf import SiteConfig, SiteConfigDefinition
from IPython import start_ipython
import click
from threading import Thread
import functools

import asyncio
from twisted.internet import asyncioreactor
from twisted.internet.base import ReactorBase
from scrapy.utils.reactor import install_reactor

install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")

import twisted.internet.reactor

reactor = cast(ReactorBase, twisted.internet.reactor)


@click.command
@click.option("--site-config", "-c", "site_config_path", type=Path, required=True)
@click.option("--verbose", "-v", "verbose", is_flag=True)
@click.option("--check-url", "-u", "debug_target_url", type=str, required=False)
def main_command(
    site_config_path: Path, verbose: bool, debug_target_url: Optional[str]
) -> None:
    return main(site_config_path, verbose, debug_target_url)


@typechecked
def main(
    site_config_cls_or_path: Union[Path, Type],
    verbose: bool,
    debug_target_url: Optional[str],
) -> None:
    configure_logging()
    settings = Settings()
    settings.setmodule(setting_definitions, priority="project")
    crawler = CrawlerRunner(settings)
    config = SiteConfig.create_by_definition(site_config_cls_or_path)

    if debug_target_url is None:
        crawler.settings.setdict(
            {
                "LOG_LEVEL": "DEBUG" if verbose else "INFO",
            },
            priority="cmdline",
        )
        d = crawler.crawl(MainSpider, config=config)
    else:
        crawler.settings.setdict(
            {
                "LOG_LEVEL": "INFO",  # DEBUG log is annoying during interactive shell
                "LOGSTATS_INTERVAL": 1440,  # 1440 min, almost not showing logs
            },
            priority="cmdline",
        )
        d = crawler.crawl(
            DebugSpider,
            config=config,
            debug_target_url=debug_target_url,
            choose_structure_definitions_callback=choose_structure_definitions,
            start_debug_callback=start_debug_repl,
        )

    run_until_done(d)


@typechecked
def choose_structure_definitions(structure_description_list: List[str]) -> int:
    prompt_message = ""
    structure_count = len(structure_description_list)
    for index, description in enumerate(structure_description_list):
        structure_number = index + 1
        prompt_message += f"[{structure_number}] {description}"
    prompt_message += "Choose structure for debug"

    choosed_number = cast(
        int, click.prompt(prompt_message, type=click.IntRange(1, structure_count))
    )
    return choosed_number - 1


@typechecked
async def start_debug_repl(user_ns: Dict[str, Any]) -> None:
    await asyncio.get_running_loop().run_in_executor(
        None, functools.partial(start_ipython_process, user_ns)
    )


@typechecked
def start_ipython_process(user_ns: Dict[str, Any]) -> None:
    start_ipython(argv=[], user_ns=user_ns)


@typechecked
def run_until_done(d: Deferred) -> None:
    result = None

    def callback(r: Any) -> None:
        nonlocal result
        result = r
        try:
            reactor.stop()
        except ReactorNotRunning:
            pass

    d.addBoth(callback)

    if not d.called:
        reactor.run()

    if result is not None and isinstance(result, Failure):
        failure = result
        if isinstance(failure.value, BaseException):
            traceback.print_exception(
                type(failure.value), failure.value, tb=failure.getTracebackObject()
            )
        failure.raiseException()


if __name__ == "__main__":
    main_command()
