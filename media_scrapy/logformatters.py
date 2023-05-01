import logging
from scrapy import logformatter, Item, Spider
from scrapy.http import Response


class LogFormatter(logformatter.LogFormatter):
    # Dropping item is a normal behavior
    def dropped(
        self, item: Item, exception: Exception, response: Response, spider: Spider
    ) -> dict:
        return {
            "level": logging.DEBUG,
            "msg": logformatter.DROPPEDMSG,
            "args": {
                "exception": exception,
                "item": item,
            },
        }
