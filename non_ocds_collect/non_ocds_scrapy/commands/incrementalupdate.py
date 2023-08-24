import os.path
from datetime import datetime

import pandas as pd
from scrapy.commands import ScrapyCommand
from scrapy.exceptions import UsageError


class IncrementalUpdate(ScrapyCommand):
    def short_desc(self):
        return (
            "Given a spider name, crawl_time and the field name to check for the dataset latest date, gets new "
            "data from the latest date until today, updating the existing file in crawl_time. Only works for "
            "spiders that inherit from ExportFileSpider with CSV as the export format."
        )

    def syntax(self):
        return "[options] [spider]"

    def add_options(self, parser):
        ScrapyCommand.add_options(self, parser)
        parser.add_argument(
            "--total_count_field",
            type=str,
            help="The data field to use for checking for the number of items downloaded the last " "time.",
        )
        parser.add_argument("--crawl_time", type=str, help="The crawl_time where previous data was stored")

    def run(self, args, opts):
        if not args:
            raise UsageError("A spider name must be set.")

        spider_name = args[0]
        if spider_name not in self.crawler_process.spider_loader.list():
            raise UsageError("The spider does not exist")

        spidercls = self.crawler_process.spider_loader.load(spider_name)

        if not hasattr(spidercls, "export_outputs"):
            raise UsageError("The selected spider must be a ExportFileSpider")

        if opts.crawl_time:
            spidercls.crawl_time = datetime.strptime(opts.crawl_time, "%Y-%m-%d %H:%M:%S")

        total_count = None
        if opts.total_count_field:
            directory = spidercls.get_file_store_directory()
            file_name = f"{spidercls.export_outputs['main']['name']}.csv"
            total_count = pd.read_csv(os.path.join(directory, file_name))[opts.total_count_field].agg(["max"])["max"]

        self.crawler_process.crawl(spidercls, last_total_count=total_count, crawl_time=opts.crawl_time)
        self.crawler_process.start()
