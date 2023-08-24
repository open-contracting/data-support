from non_ocds_scrapy.spiders.uzbekistan_base_spider import UzbekistanBaseSpider


class UzbekistanDeals(UzbekistanBaseSpider):
    name = "uzbekistan_deals"

    # ExportFileSpider
    export_outputs = {
        "main": {
            "name": "uzbekistan_deals",
            "formats": ["csv"],
            "item_filter": None,
        }
    }

    # UzbekistanBaseSpider
    base_url = "https://apietender.uzex.uz/api/common/DealsList"

    def build_filters(self, from_parameter, to_parameter, **kwargs):
        return {
            "From": from_parameter,
            "To": to_parameter,
        }
