from non_ocds_scrapy.spiders.uzbekistan_base_spider import UzbekistanBaseSpider


class UzbekistanDeals(UzbekistanBaseSpider):

    name = 'uzbekistan_deals'

    # ExportFileSpider
    export_outputs = {
        'main': {
            'name': 'uzbekistan_deals',
            'date_column': 'deal_date',
            'index': 'deal_date',
            'formats': ['json', 'csv'],
            'item_filter': None,
        }
    }

    # UzbekistanBaseSpider
    base_url = 'https://apietender.uzex.uz/api/common/DealsList'
