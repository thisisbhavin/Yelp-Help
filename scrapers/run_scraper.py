import argparse

from pandas import read_csv

from scrapy.crawler import CrawlerRunner
from scrapy.settings import SettingsAttribute
from scrapy.utils.project import get_project_settings

from twisted.internet import defer
from twisted.internet import reactor

from yelp_scraper.credentials import Crawlera
from yelp_scraper.spiders.yelp_businesses_by_location import BusinessSpider
from yelp_scraper.spiders.yelp_reviews_spider import ReviewsSpider
from yelp_scraper.spiders.yelp_menu_items_spider import MenuSpider


def main(args):
    location_list = read_csv(args.locations_file_path).location.to_list()

    settings = get_project_settings()
    # `settings` should have crawlera key, Adding key from credentials
    settings.attributes['CRAWLERA_APIKEY'] = SettingsAttribute(Crawlera.CRAWLERA_APIKEY, 20)

    runner = CrawlerRunner(settings=settings)

    @defer.inlineCallbacks
    def crawl():
        for sortby in ['recommended', 'review_count', 'rating']:
            yield runner.crawl(BusinessSpider,
                                location_list=location_list,
                                # location=location,
                                sortby=sortby,
                                pages=args.business_pages)

        for location in location_list:
            print(f'Scraping reviews/top menu items/menu urls for location - {location}')
            yield runner.crawl(ReviewsSpider,
                               location=location,
                               pages=args.reviews_pages,
                               scrape_reviews=args.scrape_reviews)

            print(f'Scraping menus for location - {location}')
            yield runner.crawl(MenuSpider,
                               location=location)

        reactor.stop()

    crawl()
    reactor.run()  # the script will block here until the last crawl call is finished


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--locations-file-path",
                        type=str,
                        help="File path containing locations")

    parser.add_argument("--scrape-reviews",
                        default=True,
                        type=lambda x: (str(x).lower() in ['true', '1', 'yes']),
                        help="Scrape reviews? (yes/no)")

    parser.add_argument("--business-pages",
                        type=int,
                        default=None,
                        help="Number of business listing pages to scrape (> 0)")

    parser.add_argument("--reviews-pages",
                        type=int,
                        default=None,
                        help="number of pages of reviews to scrape for each business (> 0)")

    args = parser.parse_args()

    if args.business_pages is not None:
        if args.business_pages < 1:
            parser.error("Number of pages must be > 0")

    if args.reviews_pages is not None:
        if args.reviews_pages < 1:
            parser.error("Number of pages must be > 0")

    main(args)
