from non_ocds_scrapy.spiders.uzbekistan_base_spider import UzbekistanBaseSpider


class UzbekistanCompletedDeals(UzbekistanBaseSpider):

    name = 'uzbekistan_completed_deals'

    # ExportFileSpider
    export_outputs = {
        'main': {
            'name': 'uzbekistan_completed_deals',
            'date_column': 'deal_date',
            'index': 'deal_date',
            'formats': ['json', 'csv'],
            'item_filter': None,
        }
    }

    # UzbekistanBaseSpider
    base_url = 'https://xarid-api-shop.uzex.uz/Common/GetCompletedDeals'

    def start_requests(self):
        for national in [1, 0]:
            filters = self.build_filters(0, self.page_size, item={'display_on_shop': 0 if national else 1,
                                                                  'display_on_national': national})
            yield self.build_request(filters, callback=self.parse_list)

    def build_filters(self, from_parameter, to_parameter, **kwargs):
        filters = super().build_filters(from_parameter, to_parameter)
        filters['display_on_shop'] = kwargs['item']['display_on_shop']
        filters['display_on_national'] = kwargs['item']['display_on_national']
        return filters
