import json

import scrapy

from non_ocds_scrapy.base_spiders.export_file_spider import ExportFileSpider


class UzbekistanBaseSpider(ExportFileSpider):

    page_size = 1000
    parse_callback = 'parse'

    # BaseSpider
    default_from_date = '2022-01-01T00:00:00'
    date_required = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.parse_callback = getattr(self, self.parse_callback)

    def start_requests(self):
        yield self.build_request(self.build_filters(0, self.page_size), callback=self.parse_list)

    def parse(self, response, **kwargs):
        for item in response.json():
            yield item

    def parse_list(self, response):
        yield from self.parse_callback(response)
        data = response.json()
        item = data[0]
        range_end = item['total_count']
        from_parameter = self.page_size + 1
        while from_parameter < range_end:
            to_parameter = from_parameter + self.page_size
            filters = self.build_filters(from_parameter, to_parameter, item=item)
            yield self.build_request(filters, self.parse_callback)
            from_parameter = to_parameter + 1

    def build_request(self, filters, callback=None):
        return scrapy.Request(
            self.base_url,
            method='POST',
            body=json.dumps(filters),
            headers={'Content-Type': 'application/json'},
            callback=callback if callback else self.parse
        )

    def build_filters(self, from_parameter, to_parameter, **kwargs):
        return {
            "from": from_parameter,
            "to": to_parameter,
            "date_from": self.from_date.strftime("%d.%m.%Y %H:%M"),
            "date_to": self.until_date.strftime("%d.%m.%Y %H:%M")
        }
