from ast import literal_eval
from bs4 import BeautifulSoup
from collections import defaultdict
from datetime import datetime
from html import unescape 
import json

import pickle
import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values
from psycopg2.extras import Json

from re import compile

from scrapy import Request
from scrapy import Spider
from scrapy.http import Response
from scrapy.utils.project import get_project_settings

from traceback import print_exc

from typing import Generator
from typing import List
from typing import Tuple
from typing import Union

from unicodedata import normalize
from urllib.parse import parse_qs
from urllib.parse import urlencode
from urllib.parse import urlparse

from yelp_scraper.credentials import Postgres
from yelp_scraper.items import Review
from yelp_scraper.utils import BusinessDetails
from yelp_scraper.utils import merge_two_dictionaries

settings = get_project_settings()



class ReviewsSpider(Spider):
    """Reviews scraper class

    ReviewsSpider provides methods to create requests, parse responses to
    get reviews and save those reviews to a GCP bucket in chunks.

    Parameters
    ----------
    **kwargs
        The keyword argument is used for getting cities list

    Attributes
    ----------
    location : str
        Name of the location to scrape reviews for
    business_data : dict
        This will store metadata for each business, like, previous reviews count,
        indexes of error in previous runs, menu url, current reviews count.
    conn
        Database connection object to business information table
    cursor
        Cursor for database
    

    Class Attributes
    ----------------
    name : str
        name of the spider.
    allowed_domains : list
        list of domains allowed to crawl by the spider.
    custom_settings : dict
        specific settings for this spider. This will override settings defined
        in `settings.py` file.

    """

    name = "Reviews_"
    allowed_domains = ["yelp.com"]
    custom_settings = {
        'ITEM_PIPELINES': {
            # 'yelp_scraper.pipelines.PostgresWriterPipeline': 400,
            'yelp_scraper.pipelines.CSPipeline': 100
        }
    }

    def __init__(self, *args, **kwargs):
        super(ReviewsSpider, self).__init__(*args, **kwargs)
        self.pages = kwargs.get('pages')
        self.location = kwargs.get('location')
        self.business_data = {}
        self.conn = None # database connection for businesses info
        self.cursor = None
        self.covid19_tags_set = set()
        self.amenities_tags_set = set()

    def get_db_connection(self):
        return psycopg2.connect(host = Postgres.PG_HOST,
                                port = Postgres.PG_PORT,
                                dbname = Postgres.PG_DBNAME,
                                user = Postgres.PG_USER,
                                password = Postgres.PG_PWD)

    def create_request(self, 
                       response_url : str, 
                       tuple_index : int, 
                       review_start_end : Tuple[int, int], 
                       business_id : str,
                       num_reviews_per_page : int = 20) -> Request:
        """Create request object for the next page

        Parameters
        ----------
        response_url : str
            URL of previous request
        tuple_index : int
            Index of error tuple from the list of errors
        review_start_end : tuple(int, int)
            Tuple of error
        business_id : str
            Business id. For example, if URL is - 
            `www.yelp.com/biz/brendas-french-soul-food-san-francisco-5` then
            id is `brendas-french-soul-food-san-francisco-5`
        num_reviews_per_page : int
            Number of reviews received in a response

        Returns
        -------
        Request
            A request object for the next page

        """
        
        # https://www.yelp.com/biz/brendas-french-soul-food-san-francisco-5/review_feed?sort_by=date_desc&start=0
        # https://www.yelp.com/biz/lJAGnYzku5zSaLnQ_T6_GQ/review_feed?rl=en&sort_by=date_desc&q=&start=20
        parsed_url = urlparse(response_url)
        query = parse_qs(parsed_url.query) 

        if query:
            start = int(query['start'][0]) + num_reviews_per_page
            referer = response_url
            base_url = response_url.split('?')[0]
        else:
            start = 0
            referer = None
            base_url = response_url + "/review_feed"

        params = {
            'rl' : 'en',
            'sort_by' : 'date_desc',
            'q': '',
            'start' : start
        }

        headers = {
            'Referer' : referer,
            'x-requested-by-react': True,
            'x-requested-with': 'XMLHttpRequest'
        }
        
        url = base_url + "?" + urlencode(params)
        
        return Request(url=url, 
                       headers=headers, 
                       callback=self.child_parse, 
                       errback=self.request_error_handler,
                       meta=dict(tuple_index=tuple_index, 
                                 review_start_end = review_start_end, 
                                 business_id = business_id))


    def get_error_requests(self, 
                           response_url : str, 
                           business_id : str) -> Generator[Request, None, None]:
        """Generator to generate initial error requests

        Parameters
        ----------
        response_url : str
            URL of previous request
        business_id : str
            Business id. For example, if URL is - 
            `www.yelp.com/biz/brendas-french-soul-food-san-francisco-5` then
            id is `brendas-french-soul-food-san-francisco-5`

        Yields
        -------
        Request
            A request object

        """
        url = response_url
        # ea = -1 represents last run scraped all then available reviews
        lrc = self.business_data[business_id].get('last_reviews_count')

        #NOTE crc from homepage sometimes do not match with crc from json response
        crc = self.business_data[business_id].get('current_reviews_count')
        delta = crc - lrc # number of new reviews since last run
        
        # Getting standard errors index
        if lrc == -1:
            # Scraping reviews for the first time for this business
            # therefore scraping crc reviews ie 0 to crc - 1
            ea = [(0, crc - 1)]
            # yield create_request(response_url)
        else:
            # lrc != -1 means that reviews has been scraped atleast once
            # for this business

            ea = self.business_data[business_id].get('errors_at')

            if delta > 0:
                # `delta` new reviews has been added
                new_reviews = [(0, delta - 1)]

                if ea != -1:
                    # Apart from fetching `delta` new reviews we will have to 
                    # get reviews where previously error occured (ea != -1)

                    # if already have [(0, ..)] then join shoft max value by 
                    # `delta`
                    try:
                        new_reviews = [max([(start, end + delta) for \
                                        start, end in ea if start == 0])]
                    except:
                        pass

                    # Shifting previous error indexes by `delta` and appending 
                    # new_reviews
                    ea = [(start + delta, end + delta) for start, end in \
                        ea if start != 0] + new_reviews

                else:
                    # last run scraped all then available reviews
                    # so, only grtting new reviews
                    ea = new_reviews

        if ea != -1:
            self.business_data[business_id]['errors_at'] = ea
        
            print(f'Errors at {ea}\n')

            for i, (start, end) in enumerate(ea):
                # `start` in url needs to be multiple of 20
                # start of an error can be anything,
                # therefore, getting nearest multiple of 20 <= `start`
                yelp_start = start - (start % 20)

                if yelp_start != 0:
                    # Only need to change `response_url` for start != 0

                    # since, `create_request` return request on the basis of 
                    # response.url, we need to use start = 20 to get request
                    # with start = 40. Therefore, `yelp_start - 20`
                    query = f'/review_feed?rl=en&sort_by=date_desc&q=&start={yelp_start - 20}'
                    url = response_url + query
                
                print(f'fetching data for {url}..')
                yield self.create_request(url, 
                                        i, 
                                        (start, end),
                                        business_id)
    

    def get_reviews_details_json(self, 
                                 reviews_info : List[dict]) -> \
                                     Generator[dict, None, None]:
        """Generator to generate reviews information

        Parameters
        ----------
        reviews_info : dict
            Dict containing reviews information as received in the response

        Yields
        ------
        review_info_dict : dict
            Parsed information about review
        """
        for review_info in reviews_info:
            review_info_dict = {}
            review_info_dict['review_id'] = review_info.get('id')

            raw_review = review_info.get('comment') \
                                    .get('text') \
                                    .replace('<br>', ' ') \
                                    .replace('</br>', ' ')

            # Convert all named and numeric character references (e.g. >, &#62;
            # , &x3e;) in the string to the corresponding unicode characters.
            review = unescape(raw_review)
            
            # convert unicode string `review` to normal form and 
            # remove any extra spaces bw sentences
            review = " ".join(normalize("NFKD", review).split())
            review_info_dict['review'] = review

            review_date = datetime.strptime(review_info.get('localizedDate'), 
                                            '%m/%d/%Y').date()
            review_info_dict['date'] = review_date
                                            
            review_info_dict['rating'] = review_info.get('rating')
            review_info_dict['business_name'] = review_info.get('business').get('name')
            review_info_dict['business_id'] = review_info.get('business').get('id')
            review_info_dict['business_alias'] = review_info.get('business').get('alias')
            review_info_dict['sentiment'] = 1 if review_info.get('rating') >= 4 else 0

            yield review_info_dict

    def start_requests(self) -> Generator[Request, None, None]:
        """Generator that generates request object for the homepage of each
        restaurant present in database filtered by a city

        Yields
        ------
        Request
            A request object for the homepage of a restaurant

        """

        try:
            self.conn = self.get_db_connection()
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        except:
            print_exc()
            exit(0)

        try:
            sql_query = (f"select business_id, "
                                  f"business_url, "
                                  f"is_business_closed, "
                                  f"year_established, "
                                  f"overall_rating, "
                                  f"num_reviews, "
                                  f"menu_url, "
                                  f"price_range, "
                                  f"phone_number, "
                                  f"num_reviews_5_stars, "
                                  f"num_reviews_4_stars, "
                                  f"num_reviews_3_stars, "
                                  f"num_reviews_2_stars, "
                                  f"num_reviews_1_star, "
                                  f"address_line1, "
                                  f"address_line2, "
                                  f"address_line3, "
                                  f"city, "
                                  f"region_code, "
                                  f"postal_code, "
                                  f"country_code, "
                                  f"operation_hours_Mon, "
                                  f"operation_hours_Tue, "
                                  f"operation_hours_Wed, "
                                  f"operation_hours_Thu, "
                                  f"operation_hours_Fri, "
                                  f"operation_hours_Sat, "
                                  f"operation_hours_Sun, "
                                  f"categories, "
                                  f"top_food_items, "
                                  f"monthly_ratings_by_year, "
                                  f"last_reviews_count, "
                                  f"errors_at "
                         f"from {Postgres.PG_TABLE_NAME} where location = '{self.location}'")

            self.cursor.execute(sql_query)

        except:
            self.cursor.execute("ROLLBACK")
            self.conn.commit()
            self.cursor.close()
            self.conn.close()
            print_exc()
            exit(0)

        db_business_data_list_dict = self.cursor.fetchall()

        if not self.conn.closed:
            self.conn.commit()
            self.cursor.close()
            self.conn.close()

        for row in db_business_data_list_dict:
            business_id = row["business_id"]
            business_url = row["business_url"]

            business_dict = {key : row[key] 
                             for key 
                             in row 
                             if key != "business_id"}

            business_dict['resolved_error_indexes'] = []
            business_dict["errors_at"] = literal_eval(business_dict.get('errors_at'))

            # MARKER - business_data[business_id] has 33 keys, 
            #            32 from db (except business_id)
            #            and `resolved_error_indexes`
            self.business_data[business_id] = business_dict

            # "https://www.yelp.com/biz/brendas-french-soul-food-san-francisco-5" 
            print(f"fetching - https://www.yelp.com{business_url}...") 

            yield Request(url = "https://www.yelp.com" + business_url, 
                          callback = self.parse,
                          meta={"business_id" : business_id})
    
    def parse(self, response : Response) -> Generator[Request, None, None]:
        """Generator function that parses homepage response.
        
        The parser scrapes following items from homepage:
            > Top food items
            > Menu URL (if present)
            > Reviews count
        
        It generates initial requests for all the errors.

        Parameters
        ----------
        response 
            Response object of the homepage

        Yields
        ------
        Request
            A request object for an error

        """
        business_id = response.meta.get("business_id")

        soup = BeautifulSoup(unescape(response.text.replace("<!--","").replace("-->","")), "html.parser")

        details_obj = BusinessDetails(soup)
        business_details_updates_dict = details_obj.get_all_updates_and_details()

        business_details_dict = business_details_updates_dict.get("business_details")
        covid19_updates_dict = business_details_updates_dict.get("covid19_updates")
        amenities_dict = business_details_updates_dict.get("amenities")

        # keeping track of all the services names, and amenities label.
        # will be used when storing these services in db
        self.covid19_tags_set = self.covid19_tags_set | set(covid19_updates_dict)
        self.amenities_tags_set = self.amenities_tags_set | set(amenities_dict)

        # merging new details with details from db
        business_details_dict = merge_two_dictionaries(self.business_data[business_id], 
                                                        business_details_dict)

        self.business_data[business_id] = {**business_details_dict,
                                           "current_reviews_count" : business_details_dict.get("num_reviews", 0),
                                           **covid19_updates_dict,
                                           **amenities_dict}

        if self.scrape_reviews:
            for req in self.get_error_requests(response.url, business_id):
                yield req

    def child_parse(self, response : Response) -> Generator[Union[Request, 
                                                                  None] ,
                                                            None, 
                                                            None]:
        """
        Parser for parsing the response object.
        Yields item object containing review infromation, which will be 
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

        """
        NOTE:
        Let's assume in the last run there was an error at review 44 till 47
        both inclusive. so,

        `reviews_start` -> 44 (Actual start index of reviews we want to scrape
                               from entire list of reviews)
        `reviews_end` -> 47 (Actual end index of reviews we want to scrape
                             from entire list of reviews)
        `start` -> request start index (In order to get reviews starting from 
                                        44 we need to make a request with highest
                                        multiple of 20 less than 44 ie 40 here)
        `end` -> request end index (Yelp sends only 20 reviews with one call,
                                    therefore `start + 19`)
        `reviews_to_get_start` -> starting index of review in list of 20 reviews.
                                  Here 44 is 4 away from 40 so, we will save 
                                  reviews from position 4 (5th review)
        `reviews_to_get_end` -> end index of review in list of 20 reviews.
                                Here 20 reviews are from 40 to 59 but `reviews_end`
                                is 47 so, we will save reviews till position 7,
                                we can get this by (20 - 59 - 47) = 8 (python slice notations!!)
        """

        response_json = json.loads(response.body_as_unicode())

        reviews_start, reviews_end = response.meta.get('review_start_end')
        tuple_index = response.meta.get('tuple_index')
        business_id = response.meta.get('business_id')

        # Actual request start
        _, start, _ = response_json.get('pagination').values()
        end = start + 19

        reviews_to_get_start = 0
        reviews_to_get_end = 20
        if start < reviews_start:
            reviews_to_get_start = (reviews_start - start)
        if end > reviews_end:
            reviews_to_get_end = (20 - (end - reviews_end))


        reviews_info = response_json.get('reviews')[reviews_to_get_start : reviews_to_get_end]
        for extracted_review_info in self.get_reviews_details_json(reviews_info):
            review_item = Review()
            review_item['review_id'] = extracted_review_info['review_id']
            review_item['review'] = extracted_review_info['review']
            review_item['date'] = extracted_review_info['date']
            review_item['rating'] = extracted_review_info['rating']
            review_item['business_name'] = extracted_review_info['business_name']
            review_item['business_id'] = extracted_review_info['business_id']
            review_item['business_alias'] = extracted_review_info['business_alias']
            review_item['business_location'] = self.location
            review_item['sentiment'] = extracted_review_info['sentiment']
            
            yield review_item

        # if self.pages is not None:
        #     if (start + num_reviews_per_page) < ((self.pages) * num_reviews_per_page):
        #         yield self.create_request(response.url, num_reviews_per_page)

        # if start of next request falls beyound total reviews to scrape then
        # stop and update `resolved_error_indexes`
        if (start + 20) < reviews_end:
            yield self.create_request(response.url,
                                      tuple_index,
                                      (reviews_start, reviews_end),
                                      business_id)
        else:
            self.business_data[business_id]['resolved_error_indexes'] += [tuple_index]

    def closed(self, reason : str) -> None:
        """This function is called when spider closes for any reason.

        Saving the status of errors in database and closing database conns.

        Parameters
        ----------
        reason : str 
            Reason for the closing of spider

        """
        covid19_amenities_tags_dict = {tag : 0 
                                       for tag 
                                       in (self.covid19_tags_set 
                                           | self.amenities_tags_set)}

        values_to_insert = []
        if self.business_data:
            for key, v in self.business_data.items():
                business_details_updates_dict = {"business_id" : key,
                                                 **covid19_amenities_tags_dict,
                                                 **v}

                # If there is an error for the very first call in `start_requests`
                # then `current_reviews_count` will not be set and 
                # since, no request has been processed `current_reviews_count` =
                # `last_reviews_count`
                last_reviews_count = (business_details_updates_dict
                                        .get('current_reviews_count'))

                errors_at = business_details_updates_dict.get('errors_at')
                if (errors_at != -1) and self.scrape_reviews:
                    errors_at = [(i,j)
                                 for k,(i,j)
                                 in enumerate(errors_at)
                                 if k not in v.get('resolved_error_indexes')]

                    if len(errors_at) == 0:
                        errors_at = -1

                errors_at = str(errors_at)

                business_details_updates_dict["last_reviews_count"] = last_reviews_count
                business_details_updates_dict["errors_at"] = errors_at
                business_details_updates_dict["monthly_ratings_by_year"] = \
                    Json(business_details_updates_dict["monthly_ratings_by_year"])

                if "resolved_error_indexes" in business_details_updates_dict.keys():
                    del business_details_updates_dict["resolved_error_indexes"]
                if "current_reviews_count" in business_details_updates_dict.keys():
                    del business_details_updates_dict["current_reviews_count"]

                if not self.scrape_reviews:
                    # this is also the reason why we have `num_reviews` and 
                    # `last_reviews_count`:
                    # `num_reviews` is latest but `last_reviews_count` can be any
                    # historical value
                    # if no reviews are to be scraped, 
                    # no need to update `last_reviews_count`
                    _ = business_details_updates_dict.pop("last_reviews_count")

                values_to_insert.append(business_details_updates_dict)

        with open(f"reviews_spider_update_data_{datetime.now().strftime('%b_%d_%Y_%H_%M')}.pickle", "wb") as f:
            pickle.dump(values_to_insert, f)

        try:
            self.conn = self.get_db_connection()
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except:
            print_exc()
            exit(0)

        try:
            # fetch all column names
            self.cursor.execute((f"select column_name "
                                 f"from information_schema.columns "
                                 f"where table_name = '{Postgres.PG_TABLE_NAME}'"))
            
            column_names_set = set([column_name[0] for column_name in self.cursor.fetchall()])

            columns_to_add_to_table = (self.covid19_tags_set 
                                       | self.amenities_tags_set) - column_names_set

            if columns_to_add_to_table:
                self.cursor.execute((f'ALTER TABLE {Postgres.PG_TABLE_NAME} {", ".join([f"ADD COLUMN {column} int4 NULL DEFAULT 0"  for column in columns_to_add_to_table])}'))

            if values_to_insert:
                execute_values(self.cursor,
                               (f'INSERT INTO {Postgres.PG_TABLE_NAME} ({", ".join(values_to_insert[0].keys())}) '
                                f'VALUES %s '
                                f'ON CONFLICT (business_id) '
                                f'DO UPDATE SET {", ".join(["".join([str(k), " = excluded.", str(k)]) for k in values_to_insert[0].keys()])}'),
                               [[value for value in data_dict.values()] for data_dict in values_to_insert])
        except:
            print_exc()

        finally:
            self.conn.commit()
            self.cursor.close()
            self.conn.close()

    def request_error_handler(self, failure) -> None:
        """This function is called when error occurs in processing any request.
        Update start of an error (if required).

        Parameters
        ----------
        failure : Failure 
            Failure object

        """

        meta = failure.value.response.meta
        reviews_start, reviews_end = meta.get('review_start_end')
        tuple_index = meta.get('tuple_index')
        business_id = meta.get('business_id')

        response_url = failure.value.response.url
        query = parse_qs(urlparse(response_url).query)
        start = int(query['start'][0])

        # if the start of request is greater than error start then that means
        # that there was atleast one successful request and error starting index
        # need to be changed.
        # Keep in mind that error end point do not change!
        if start > reviews_start:
            self.business_data[business_id]['errors_at'][tuple_index] = (start, reviews_end)