import scrapy


class MediaFiles(scrapy.Item):
    file_urls = scrapy.Field()
    file_paths = scrapy.Field()
    files = scrapy.Field()
