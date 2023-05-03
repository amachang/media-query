from pathlib import Path
from scrapy.settings import Settings
from scrapy.crawler import CrawlerRunner
from media_scrapy import settings as setting_definitions
from media_scrapy.spiders import MainSpider
from twisted.python.failure import Failure
from tap import Tap
from scrapy.utils.log import configure_logging
from typing import Any, Optional, cast
import traceback
from typeguard import typechecked
from twisted.internet.defer import Deferred
from twisted.internet.error import ReactorNotRunning

import asyncio
from twisted.internet import asyncioreactor
from twisted.internet.base import ReactorBase
from scrapy.utils.reactor import install_reactor

install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")

import twisted.internet.reactor

reactor = cast(ReactorBase, twisted.internet.reactor)


@typechecked
class Args(Tap):
    site_config: Path
    verbose: bool = False
    check_url: Optional[str] = None

    def configure(self) -> None:
        self.add_argument("-c", "--site-config", required=True)
        self.add_argument("-v", "--verbose", action="store_true", required=False)
        self.add_argument("-u", "--check-url", type=str, required=False)


@typechecked
def main(args: Args) -> None:
    configure_logging()
    settings = Settings()
    settings.setmodule(setting_definitions, priority="project")
    settings.setdict(
        {
            "LOG_LEVEL": "DEBUG" if args.verbose else "INFO",
        },
        priority="cmdline",
    )
    crawler = CrawlerRunner(settings)

    d = crawler.crawl(
        MainSpider, siteconf=args.site_config, debug_target_url=args.check_url
    )

    run_until_done(d)


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
    args = Args().parse_args()
    main(args)
