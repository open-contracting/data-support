class UzbekistanAuctionFilter:
    def __init__(self, feed_options):
        self.feed_options = feed_options

    def accepts(self, item):
        if "product_name" in item:
            return False
        return True


class UzbekistanAuctionProductFilter:
    def __init__(self, feed_options):
        self.feed_options = feed_options

    def accepts(self, item):
        if "product_name" in item:
            return True
        return False
