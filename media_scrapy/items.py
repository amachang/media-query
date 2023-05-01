import scrapy
from typeguard import typechecked


@typechecked
class DownloadUrlItem(scrapy.Item):
    url = scrapy.Field()
    file_path = scrapy.Field()
    tmp_file_path = scrapy.Field()


@typechecked
class SaveFileContentItem(scrapy.Item):
    file_path = scrapy.Field()
    file_content = scrapy.Field()
