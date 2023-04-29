import scrapy
from typeguard import typechecked


@typechecked
class MediaFiles(scrapy.Item):
    file_urls = scrapy.Field()
    file_paths = scrapy.Field()
    file_contents = scrapy.Field()
    files = scrapy.Field()
