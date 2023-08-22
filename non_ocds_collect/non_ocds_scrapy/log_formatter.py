# https://docs.scrapy.org/en/latest/topics/logging.html#custom-log-formats
from scrapy.logformatter import LogFormatter as _LogFormatter


class LogFormatter(_LogFormatter):
    # https://docs.scrapy.org/en/latest/_modules/scrapy/logformatter.html#LogFormatter.scraped
    def scraped(self, item, *args):
        return None

    # https://docs.scrapy.org/en/latest/_modules/scrapy/logformatter.html#LogFormatter.dropped
    def dropped(self, item, *args):
        return None
