import scrapy

from non_ocds_scrapy.spiders.uzbekistan_base_spider import UzbekistanBaseSpider


class UzbekistanDirectPurchases(UzbekistanBaseSpider):

    name = 'uzbekistan_direct_purchases'

    # ExportFileSpider
    export_outputs = {
        'main': {
            'name': 'uzbekistan_direct_purchases',
            'formats': ['json', 'csv'],
            'item_filter': None,
            # This endpoint doesn't support date filters, so we need to get all the data everytime (no incremental
            # updates supported).
            'overwrite': True,
        }
    }

    # UzbekistanBaseSpider
    base_url = 'https://xarid-api-purchase.uzex.uz/Common/GetDirectPurchases'
    date_required = False
    parse_callback = 'parse_direct_purchases'

    def parse_direct_purchases(self, response, **kwargs):
        for item in response.json():
            yield scrapy.Request(f"https://xarid-api-purchase.uzex.uz/Common/GetDirectPurchase/{item['id']}",
                                 callback=self.parse_direct_purchase_details)

    def parse_direct_purchase_details(self, response, **kwargs):
        yield response.json()

    def build_filters(self, from_parameter, to_parameter, **kwargs):
        return {
            "from": from_parameter,
            "to": to_parameter,
        }
