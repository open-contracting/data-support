import csv
import json
import os
from datetime import datetime

import psycopg2.sql
from psycopg2 import sql
from scrapy import signals
from scrapy.exceptions import NotConfigured


class DatabaseStore:
    """
    If the ``DATABASE_URL`` Scrapy setting is set and the ``ExportFileSpider`` (with the jsonline output format) is
    used, the data is stored in a PostgreSQL database, incrementally.

    This extension stores data in the "data" column of a table named after the ``spider.export_outputs.name`` dict.
    When the spider is opened, if the ``spider.export_outputs.main.name`` table doesn't exist, it is created.
    The spider's ``from_date`` attribute is then set, in order of precedence, to: the ``from_date`` spider argument
    (unless equal to the spider's ``default_from_date`` class attribute); the maximum value of the ``date`` field of
    the stored data (if any); the spider's ``default_from_date`` class attribute (if set).

    When the spider is closed, this extension reads the data written by the Feeds extension. Then, it recreates the
    table, and inserts each spiders output.

    To perform incremental updates, the data in the export directory must not be deleted between crawls.
    """


    def __init__(self, database_url):
        self.database_url = database_url

        self.connection = None
        self.cursor = None

    @classmethod
    def from_crawler(cls, crawler):
        database_url = crawler.settings['DATABASE_URL']
        directory = crawler.settings['FILES_STORE']

        if not database_url:
            raise NotConfigured('DATABASE_URL is not set.')
        if not directory:
            raise NotConfigured('FILES_STORE is not set.')

        extension = cls(database_url)

        crawler.signals.connect(extension.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(extension.spider_closed, signal=signals.spider_closed)

        return extension

    def spider_opened(self, spider):
        self.connection = psycopg2.connect(self.database_url)
        self.cursor = self.connection.cursor()
        try:
            table_name = spider.export_outputs['main']['name']
            self.create_table(table_name)

            # If there is not a from_date from the command line or the from_date is equal to the default_from_date,
            # get the most recent date in the spider's data table.
            if getattr(spider, 'default_from_date', None):
                default_from_date = datetime.strptime(spider.default_from_date, spider.date_format)
            else:
                default_from_date = None

            if 'date_column' in spider.export_outputs['main'] and (not spider.from_date or
                                                                   spider.from_date == default_from_date):
                spider.logger.info('Getting the date from which to resume the crawl from the %s table', table_name)
                self.execute("SELECT max(data->>'{date_column}')::timestamptz FROM {table}", table=table_name,
                             date_column=sql.SQL(spider.export_outputs['main']['date_column']))
                from_date = self.cursor.fetchone()[0]
                if from_date:
                    formatted_from_date = datetime.strftime(from_date, spider.date_format)
                    spider.logger.info('Resuming the crawl from %s', formatted_from_date)
                    spider.from_date = datetime.strptime(formatted_from_date, spider.date_format)

            self.connection.commit()
        finally:
            self.cursor.close()
            self.connection.close()

    def spider_closed(self, spider, reason):
        if reason != 'finished':
            return

        crawl_directory = spider.get_file_store_directory()
        spider.logger.info('Reading the %s crawl directory', crawl_directory)

        for table in spider.export_outputs.keys():
            table_name = spider.export_outputs[table]['name']
            filename = os.path.join(crawl_directory, f'{spider.export_outputs[table]["name"]}.json')
            csv_filename = os.path.join(crawl_directory, 'data.csv')
            spider.logger.info('Writing the JSON data to the %s CSV file', filename)
            with open(csv_filename, 'w') as f:
                writer = csv.writer(f)
                for line in self.yield_items_from_file(filename):
                    writer.writerow([json.dumps(line)])

            spider.logger.info('Replacing the JSON data in the %s table', table_name)
            self.connection = psycopg2.connect(self.database_url)
            self.cursor = self.connection.cursor()
            try:
                self.execute('DROP TABLE IF EXISTS {table}', table=table_name)
                self.create_table(table_name)
                with open(csv_filename) as f:
                    self.cursor.copy_expert(self.format('COPY {table}(data) FROM STDIN WITH CSV', table=table_name), f)

                if 'index' in spider.export_outputs[table]:
                    self.execute("CREATE INDEX {index} ON {table}(cast(data->>'{field_index}' as text))",
                                 table=table_name, index=f'idx_{table_name}',
                                 field_index=sql.SQL(spider.export_outputs[table]['index']))
                self.connection.commit()
            finally:
                self.cursor.close()
                self.connection.close()
                os.remove(csv_filename)

    def create_table(self, table):
        self.execute('CREATE TABLE IF NOT EXISTS {table} (data jsonb)', table=table)

    def yield_items_from_file(self, file_name):
        with open(file_name, 'r') as f:
            for number, line in enumerate(f):
                yield json.loads(line)

    # Copied from kingfisher-summarize
    def format(self, statement, **kwargs):
        """
        Formats the SQL statement, expressed as a format string with keyword arguments. A keyword argument's value is
        converted to a SQL identifier, or a list of SQL identifiers, unless it's already a ``sql`` object.
        """
        objects = {}
        for key, value in kwargs.items():
            if isinstance(value, psycopg2.sql.Composable):
                objects[key] = value
            elif isinstance(value, list):
                objects[key] = psycopg2.sql.SQL(', ').join(psycopg2.sql.Identifier(entry) for entry in value)
            else:
                objects[key] = psycopg2.sql.Identifier(value)
        return psycopg2.sql.SQL(statement).format(**objects)

    # Copied from kingfisher-summarize
    def execute(self, statement, variables=None, **kwargs):
        """
        Executes the SQL statement.
        """
        if kwargs:
            statement = self.format(statement, **kwargs)
        self.cursor.execute(statement, variables)
