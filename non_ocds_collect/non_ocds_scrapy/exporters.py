# https://stackoverflow.com/a/34487833
from scrapy.exporters import CsvItemExporter


class HeadlessCsvItemExporter(CsvItemExporter):
    def __init__(self, file, *args, **kwargs):
        # Include the header line if the current stream position is the start of the file.
        kwargs["include_headers_line"] = file.tell() == 0
        super().__init__(file, *args, **kwargs)
