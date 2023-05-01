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
    "media_scrapy.pipelines.DropUnneededItemPipeline": 100,
    "media_scrapy.pipelines.SaveFileContentPipeline": 200,
    "media_scrapy.pipelines.PrepareItemForFilesPipelines": 300,
    "scrapy.pipelines.files.FilesPipeline": 400,
    "media_scrapy.pipelines.SaveDownloadedFilePipeline": 500,
}

LOG_FORMATTER = "media_scrapy.logformatters.LogFormatter"

FILES_STORE = path.join(gettempdir(), "media_scrapy")
FILES_EXPIRES = 0

REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
FEED_EXPORT_ENCODING = "utf-8"

DOWNLOAD_WARNSIZE = 1 * 1024 * 1024 * 1024
