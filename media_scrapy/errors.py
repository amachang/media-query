from typeguard import typechecked


@typechecked
class MediaScrapyError(Exception):
    pass
