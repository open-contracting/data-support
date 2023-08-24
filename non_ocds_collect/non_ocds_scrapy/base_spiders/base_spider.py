from datetime import datetime

import scrapy
from scrapy.exceptions import UsageError


class BaseSpider(scrapy.Spider):
    """
    With respect to the data's source:

    -  If the source can support ``from_date`` and ``until_date`` spider arguments:

       -  Set a ``date_format`` class attribute to "date", "datetime", "year" or "year-month" (default "date").
       -  Set a ``default_from_date`` class attribute to a date ("YYYY-MM-DD"), datetime ("YYYY-MM-DDTHH:MM:SS"),
          year ("YYYY") or year-month ("YYYY-MM").
       -  If the source stopped publishing, set a ``default_until_date`` class attribute to a date or datetime.

    -  If the spider requires date parameters to be set, add a ``date_required = True`` class attribute, and set the
       ``date_format`` and ``default_from_date`` class attributes as above.

    .. tip::

        If ``date_required`` is ``True``, or if either the ``from_date`` or ``until_date`` spider arguments are set,
        then ``from_date`` defaults to the ``default_from_date`` class attribute, and ``until_date`` defaults to the
        ``get_default_until_date()`` return value (which is the current time, by default).

    """
    VALID_DATE_FORMATS = {'date': '%Y-%m-%d', 'datetime': '%Y-%m-%dT%H:%M:%S', 'year': '%Y', 'year-month': '%Y-%m'}

    # Regarding the data source.
    date_format = 'datetime'
    date_required = False

    def __init__(self, from_date=None, until_date=None, crawl_time=None, last_total_count=None, *args, **kwargs):
        """
        :param from_date: the date from which to download data (see :ref:`spider-arguments`)
        :param until_date: the date until which to download data (see :ref:`spider-arguments`)
        :param crawl_time: override the crawl's start time
        """

        super().__init__(*args, **kwargs)

        # https://docs.scrapy.org/en/latest/topics/spiders.html#spider-arguments

        # Related to filtering data from the source.
        self.from_date = from_date
        self.until_date = until_date

        self.date_format = self.VALID_DATE_FORMATS[self.date_format]

        # Related to incremental crawls.
        if crawl_time:
            self.crawl_time = crawl_time
        else:
            self.crawl_time = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        self.last_total_count = last_total_count

        spider_arguments = {
            'from_date': from_date,
            'until_date': until_date,
            'crawl_time': crawl_time,
            'last_total_count': last_total_count,
        }
        spider_arguments.update(kwargs)

        self.logger.info('Spider arguments: %r', spider_arguments)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BaseSpider, cls).from_crawler(crawler, *args, **kwargs)

        try:
            spider.crawl_time = datetime.strptime(spider.crawl_time, '%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            raise UsageError(f'spider argument `crawl_time`: invalid date value: {e}')

        if spider.from_date or spider.until_date or spider.date_required:
            if not spider.from_date:
                spider.from_date = spider.default_from_date
            try:
                if isinstance(spider.from_date, str):
                    spider.from_date = datetime.strptime(spider.from_date, spider.date_format)
            except ValueError as e:
                raise UsageError(f'spider argument `from_date`: invalid date value: {e}')

            if not spider.until_date:
                spider.until_date = cls.get_default_until_date(spider)
            try:
                if isinstance(spider.until_date, str):
                    spider.until_date = datetime.strptime(spider.until_date, spider.date_format)
            except ValueError as e:
                raise UsageError(f'spider argument `until_date`: invalid date value: {e}')

        return spider

    @classmethod
    def get_default_until_date(cls, spider):
        """
        Returns the ``default_until_date`` class attribute if truthy. Otherwise, returns the current time.
        """
        if getattr(spider, 'default_until_date', None):
            return spider.default_until_date
        return datetime.utcnow()

    def get_start_time(self):
        """
        Returns the formatted start time of the crawl.
        """
        if self.crawl_time:
            date = self.crawl_time
        else:
            date = self.crawler.stats.get_value('start_time')
        return date.strftime('%Y%m%d_%H%M%S')
