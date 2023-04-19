from pathlib import Path
from scrapy.settings import Settings
from scrapy.crawler import CrawlerRunner
from media_scrapy import settings as setting_definitions
from media_scrapy.spiders import MainSpider
from twisted.internet import reactor
from twisted.python.failure import Failure
from tap import Tap
from scrapy.utils.log import configure_logging
from typing import Any


class Args(Tap):
    site_config: Path

    def configure(self) -> None:
        self.add_argument("-c", "--site_config")


def main(args: Args) -> None:
    configure_logging()
    settings = Settings()
    settings.setmodule(setting_definitions, priority="project")
    crawler = CrawlerRunner(settings)
    deferred = crawler.crawl(MainSpider, siteconf=args.site_config)

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
        failure.raiseException()


if __name__ == "__main__":
    args = Args().parse_args()
    main(args)