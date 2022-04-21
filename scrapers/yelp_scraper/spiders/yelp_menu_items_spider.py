from bs4 import BeautifulSoup
from datetime import datetime

from pickle import dump as pickle_dump
import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values
from psycopg2.extras import Json

from scrapy import Request
from scrapy import Spider
from scrapy.http import Response
from scrapy.utils.project import get_project_settings

from traceback import print_exc

from typing import Generator
from typing import Union

from yelp_scraper.credentials import Postgres
from yelp_scraper.utils import get_menu

settings = get_project_settings()


class MenuSpider(Spider):
    """Menu scraper class

    MenuSpider provides methods to create requests, parse responses to
    get menu items of a restaurant (only for business that has uploaded the menu)

    Parameters
    ----------
    **kwargs
        The keyword argument is used for getting city (location)

    Attributes
    ----------
    location : str
        Name of the location to scrape reviews for
    business_data : dict
        This will store menu_items_scraped_flag for each business.
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

    name = "Menu"
    allowed_domains = ["yelp.com"]

    def __init__(self, *args, **kwargs):
        super(MenuSpider, self).__init__(*args, **kwargs)
        self.location = kwargs.get('location')
        self.menu_data = {}
        self.conn = None # database connection
        self.cursor = None

    def get_db_connection(self):
        return psycopg2.connect(host = Postgres.PG_HOST,
                                port = Postgres.PG_PORT,
                                dbname = Postgres.PG_DBNAME,
                                user = Postgres.PG_USER,
                                password = Postgres.PG_PWD)

    def start_requests(self) -> Generator[Request, None, None]:
        """Generator that generates request object for the menu page of each
        restaurant present in database filtered by a city

        Yields
        ------
        Request
            A request object for the menu page of a restaurant

        """

        try:
            self.conn = self.get_db_connection()
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except:
            print_exc()
            exit(0)

        try:
            self.cursor.execute((f"select business_id, menu_url "
                                 f"from {Postgres.PG_TABLE_NAME} "
                                 f"where location = '{self.location}' "
                                         f"and menu_url is not null "
                                         f"and menu_items_scraped_flag = 0"))

        except:
            self.cursor.execute("ROLLBACK")
            self.conn.commit()
            self.cursor.close()
            self.conn.close()
            print_exc()
            exit(0)

        db_menu_url_list_dict = self.cursor.fetchall()

        if self.conn:
            self.conn.commit()
            self.cursor.close()
            self.conn.close()

        for row in db_menu_url_list_dict:
            business_id = row.get("business_id")
            menu_url = row.get("menu_url")

            self.menu_data[business_id] = {"menu" : {}}

            # "https://www.yelp.com/menu/ramuntos-brick-oven-pizza-williston-williston"
            print(f"Fetching {menu_url}")
            yield Request(url="https://www.yelp.com" + menu_url, 
                          callback=self.parse,
                          meta={"business_id":business_id,
                                "menu_url":menu_url})
    
    def parse(self, response : Response) -> Generator[Union[Request, 
                                                            None] ,
                                                      None,
                                                      None]:
        """Generator function that parses menu page response.
        
        The parser scrapes following items from menu page:
            > Categories of dishes
            > Dishes within each category
            > Description of dishes - mostly ingredients (if available)

        Parameters
        ----------
        response 
            Response object of the menu page

        Yields
        ------
        Menu
            A Menu object for the postgres pipeline

        """

        business_id = response.meta.get("business_id")
        db_menu_url = response.meta.get("menu_url")
        request_url = response.url

        soup = BeautifulSoup(response.text, "html.parser")

        try:
            scraped_menu = get_menu(soup)
        except:
            print()
            print(f"Invalid manu url - {request_url}")
            print()
            scraped_menu = None
            self.menu_data[business_id]["menu"] = None
            self.menu_data[business_id]["menu_url"] = None
            self.menu_data[business_id]["menu_items_scraped_flag"] = 0

        if scraped_menu is not None:
            try:
                sub_menu_name_list = [i.text.strip().lower() 
                                    for i 
                                    in soup.select('.sub-menus li')]

                if sub_menu_name_list:
                    sub_menu_name = "-".join((sub_menu_name_list[0]
                                                    .lower()
                                                    .replace("/", "-")
                                                    .split()))
                else:
                    sub_menu_name = "menu"
            except:
                sub_menu_name = "menu"

            self.menu_data[business_id]["menu"][sub_menu_name] = scraped_menu
            self.menu_data[business_id]["menu_url"] = db_menu_url
            self.menu_data[business_id]["menu_items_scraped_flag"] = 1

            try:
                sub_menu_url_list = [i['href'] 
                                    for i 
                                    in soup.select('.sub-menus li a')]
            except:
                sub_menu_url_list = []

            for sub_menu_url in sub_menu_url_list:
                print(f"sub menu {sub_menu_url}")
                yield Request(url="https://www.yelp.com" + sub_menu_url, 
                            callback=self.child_parse,
                            meta={"business_id":business_id})

    def child_parse(self, response : Response):
        """
        Parameters
        ----------
        response 
            Response object of the menu page

        """

        business_id = response.meta.get('business_id')
        try:
            sub_menu_name = response.url.rsplit("/", 1)[-1]

            soup = BeautifulSoup(response.text, "html.parser")

            scraped_menu = get_menu(soup)

            if scraped_menu:
                self.menu_data[business_id]["menu"][sub_menu_name] = scraped_menu
        except:
            pass
        
    def closed(self, reason : str) -> None:
        """This function is called when spider closes for any reason.

        Saving the `menu_items_scraped_flag` and `menu` in the database.

        Parameters
        ----------
        reason : str 
            Reason for the closing of spider

        """
        if self.menu_data:
            values_to_insert = []
            for k, v in self.menu_data.items():
                # NOTE:
                # values_to_insert -> [(business_id, 
                #                       business_url, 
                #                       menu_url,
                #                       menu_items_scraped_flag, 
                #                       menu)]
                # Since `business_url` is `not null` column in db, 
                # "", acts as placeholder, it will not overwrite value in db
                if v.get("menu", {}) is None:
                    menu = None
                else:
                    menu = Json(v.get("menu", {}))

                values_to_insert.extend([(k, 
                                          "",
                                          v.get("menu_url"),
                                          v.get("menu_items_scraped_flag", 0), 
                                          menu)])

            with open(f"menu_spider_update_data_{datetime.now().strftime('%b_%d_%Y_%H_%M')}.pickle", "wb") as f:
                pickle_dump(values_to_insert, f)

            try:
                self.conn = self.get_db_connection()
                self.cursor = self.conn.cursor()
            except:
                print_exc()
                exit(0)

            try:
                execute_values(self.cursor, 
                               (f"INSERT INTO {Postgres.PG_TABLE_NAME} (business_id, "
                                                                f"business_url, "
                                                                f"menu_url, "
                                                                f"menu_items_scraped_flag, "
                                                                f"menu) "
                                f"VALUES %s "
                                f"ON CONFLICT (business_id) "
                                f"DO UPDATE SET menu_url = excluded.menu_url, "
                                                f"menu_items_scraped_flag = excluded.menu_items_scraped_flag, "
                                                f"menu = excluded.menu"), 
                                values_to_insert)
            except:
                print_exc()

            finally:
                self.conn.commit()
                self.cursor.close()
                self.conn.close()
