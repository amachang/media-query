from pathlib import Path
from scrapy.settings import Settings
from scrapy.crawler import CrawlerRunner
from media_scrapy import settings as setting_definitions
from media_scrapy.spiders import MainSpider
from twisted.internet import reactor
from twisted.python.failure import Failure
from tap import Tap
from scrapy.utils.log import configure_logging
from typing import Any, Optional
import traceback
from typeguard import typechecked


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
    deferred = crawler.crawl(
        MainSpider, siteconf=args.site_config, debug_target_url=args.check_url
    )

    finished = False
    failure = None

    def callback(result: Any) -> None:
        nonlocal finished, failure
        if isinstance(result, Failure):
            failure = result
        finished = True

        try:
            assert hasattr(reactor, "stop")
            reactor.stop()
        except:
            pass

    deferred.addBoth(callback)

    if not finished:
        assert hasattr(reactor, "run")
        reactor.run()

    if failure is not None:
        if isinstance(failure.value, BaseException):
            traceback.print_exception(
                type(failure.value), failure.value, tb=failure.getTracebackObject()
            )
        failure.raiseException()


if __name__ == "__main__":
    args = Args().parse_args()
    main(args)
