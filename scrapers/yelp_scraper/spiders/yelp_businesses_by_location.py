from json import loads as json_loads
from pandas import read_csv
from scrapy import Request
from scrapy import Spider
from scrapy.http import Response

from traceback import print_exc
from typing import Generator
from typing import Union

from urllib.parse import parse_qs
from urllib.parse import urlencode
from urllib.parse import urlparse

from yelp_scraper.items import Business

class BusinessSpider(Spider):
    """Business information scraper class

    BusinessSpider provides methods to create requests, parse responses to
    get useful information and save that information to PostgreSQL.

    Parameters
    ----------
    location : str
        Name of the location
    pages : int or None
        Scrape first `pages` number of pages

    Attributes
    ----------
    location : str
        Name of the location
    pages : int or None
        Scrape first `pages` number of pages

    Class Attributes
    ----------------
    name : str
        name of the spider.
    allowed_domains : list
        list of domains allowed to crawl by the spider.
    custom_settings : dict
        specific settings for this spider. This will override settings defined
        in `settings.py` file.
    business_reviews_cutoff : int (>= 0)
        Store only businesses with more than `business_reviews_cutoff` reviews.
    total_results : int
        Total restaurants listing for a location
    results_per_page : int
        Number of results per response page
    errors_after_1000_listings : int
        This is used to keep track of number of errors after 1000 restaurant 
        listing. Yelp usually only serve upto 1000 listings even if 
        `total_results` is more than that. If this is more than 3, give up
        subsequent requests

    """
    # X-Crawlera-Error analyse this in response 
    # header to check for banned proxies
    name = "Businesses"
    allowed_domains = ["yelp.com"]
    custom_settings = {
        'ITEM_PIPELINES': {
            'yelp_scraper.pipelines.PostgresWriterPipeline': 400
        }
    }
    business_reviews_cutoff = 0
    total_results = None
    results_per_page = None
    errors_after_1000_listings = 0

    def start_requests(self) -> Generator[Request, None, None]:
        """Generator that generates request object for first page of each city
        from `cities`

        Yields
        ------
        Request
            A request object for the first page of a city

        """
        location_list = self.location_list
        url_list = []

        for location in location_list:
            print(f"location - {location}")

            # generate start url for the given location
            # choq -> include related restaurants from nearby places
            params = {
                'cflt' : 'restaurants',
                # 'choq': 1,
                # 'find_desc': "Panera Bread",
                'find_loc' : location,
                'sortby' : self.sortby,
                'start' : 0,
                'request_origin' : 'user'
            }

            headers = {
                'Referer' : (f'https://www.yelp.com/search?cflt=restaurants&'
                            f'find_desc=&find_loc={"%20".join(location.split())}'
                            f'sortby={self.sortby}'
                            f'&start=0')
            }

            url = f"https://www.yelp.com/search/snippet?{urlencode(params)}"
            print(f"fetching - {url}")
            url_list.append(Request(url=url, headers=headers, callback=self.child_parse))
        
        return url_list 
    
    def create_request(self, 
                       query : dict, 
                       results_per_page : int) -> Request:
        """Create request object for the next page

        Parameters
        ----------
        query : dict
            query string from the previous request, in dictionary format
        results_per_page : int
            Number of businesses data received in the previous request

        Returns
        -------
        Request
            A request object for the next page

        """
        
        params = {
            'cflt' : 'restaurants',
            # 'choq': 1,
            # 'find_desc': "Panera Bread",
            'find_loc' : query['find_loc'][0],
            'sortby' : self.sortby,
            'start' : int(query['start'][0]) + results_per_page,
            'request_origin' : 'user'
        }

        headers = {
            'Referer' : (f'https://www.yelp.com/search?cflt=restaurants&'
                         f'find_desc=&find_loc='
                         f'{"%20".join(query["find_loc"][0].split())}'
                         f'sortby={self.sortby}'
                         f'start={int(query["start"][0]) + results_per_page}')
        }
        
        url = f"https://www.yelp.com/search/snippet?{urlencode(params)}"
        print(f"fetching - {url}")
        return Request(url=url, headers=headers, callback=self.child_parse)


    def child_parse(self, response : Response) -> Generator[Union[Request, 
                                                                  None] ,
                                                            None, 
                                                            None]:
        """Parser for parsing the response object and getting the business info.
        Yields item object containing business infromation, which will be 
        captured by pipeline defined in `custom_settings`

        Parameters
        ----------
        response 
            Response object

        Yields
        ------
        Request
            A request object for the next page

        """
        # get query string from the url
        query = parse_qs(urlparse(response.url).query)

        # response object is JSON, not html
        try:
            response = json_loads(response.body_as_unicode())

            search_results_list = (response.get("searchPageProps")
                                           .get("mainContentComponentsListProps"))

            business_dict_list = [search_result
                                  for search_result
                                  in search_results_list
                                  if "bizId" in search_result.keys()]

            # print(f"\nbusiness_dict_list - {business_dict_list}\n")

            for business_dict in business_dict_list:
                try:
                    business_info = business_dict.get("searchResultBusiness")
                    # print(f"business_info-{business_info}")

                    is_ad = business_info.get("isAd")
                    # if can't find find reviews, don't blow the flow
                    num_reviews = business_info.get('reviewCount', 0)

                    # Remove ads and consider business only if num_reviews >= cutoff
                    if (not is_ad) and (num_reviews >= self.business_reviews_cutoff):
                        # The order of this dictionary cannot be changed.
                        # any change in order should reflect in 
                        # `pipelines.PostgresWriterPipeline.process_item` method
                        # The reason is because this dict is being converted to tuple
                        # while storing data into db. Conversion to tuple is necessary
                        # because we are storing data in bulk and api for doing that
                        # expects data as list of tuples

                        business_item = Business()
                        business_item['business_id'] = business_dict["bizId"]

                        # NOTE: some businesses have " - Temp. CLOSED" with their name
                        # ex. "Tropisueno - Temp. CLOSED" or "The House - Temp. CLOSED"
                        # removing that
                        business_name = business_info.get("name")
                        if business_name:
                            business_name = business_name.replace(" - Temp. CLOSED", "").replace(" - CLOSED", "")
                        business_item['business_name'] = business_name

                        business_item['overall_rating'] = business_info.get("rating")
                        business_item['business_url'] = business_info["businessUrl"].split("?osq=")[0]
                        business_item['num_reviews'] = num_reviews

                        business_item['location'] = query.get('find_loc', [None])[0]

                        business_item['categories'] = [cat.get("title") 
                                                       for cat 
                                                       in business_info.get("categories", 
                                                                            [])]

                        business_item['phone_number'] = business_info.get("phone")
                        business_item["address_line1"] = business_info.get("formattedAddress")

                        # print(f"\nitem - {business_item}\n")
                        yield business_item
                except:
                    print_exc()
                    continue

            pagination_dict = [search_result
                                for search_result
                                in search_results_list
                                if search_result.get("type", "biz") == "pagination"][0]
            pagination_dict = pagination_dict.get("props")

            # pagination_dict = (response.get('searchPageProps')
            #                            .get('mainContentComponentsListProps')
            #                            .get('paginationInfo'))
            print(pagination_dict)
            self.total_results = int(pagination_dict.get('totalResults'))
            self.results_per_page = int(pagination_dict.get('resultsPerPage'))

        except:
            print("Exception!!")
            print_exc()
            if int(query.get('start')[0]) >= 1000:
                # usually yelp do not serve listing above 1000
                self.errors_after_1000_listings += 1

        finally:
            # url for any subsequent request
            # https://www.yelp.com/search/snippet?find_loc=San%20Francisco&sortby=review_count&start=10&parent_request_id=7ff323db7eff95da&request_origin=user
            if self.total_results and (self.errors_after_1000_listings < 3):
                self.errors_after_1000_listings = 0
                start = int(query.get('start')[0])

                if self.pages is not None:
                    if (((start + self.results_per_page) < self.total_results)
                            and (((start / self.results_per_page) + 1) < self.pages)):
                        yield self.create_request(query, self.results_per_page)

                elif (start + self.results_per_page) < self.total_results:
                    yield self.create_request(query, self.results_per_page)