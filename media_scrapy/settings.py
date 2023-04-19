from tempfile import gettempdir
from os import path

BOT_NAME = "media_scrapy"
SPIDER_MODULES = ["media_scrapy.spiders"]
ROBOTSTXT_OBEY = False
CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 2

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36"

# COOKIES_DEBUG = True

ITEM_PIPELINES = {
    "media_scrapy.pipelines.DropDupeMediaFilesPipeline": 100,
    "scrapy.pipelines.files.FilesPipeline": 200,
    "media_scrapy.pipelines.SaveMediaFilesPipeline": 300,
}

FILES_STORE = path.join(gettempdir(), "media_scrapy")

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
# TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

LOG_LEVEL = "INFO"
DOWNLOAD_WARNSIZE = 1 * 1024 * 1024 * 1024
