import json

from re import compile
from re import sub
import unidecode
from traceback import print_exc


def remove_any_brackets(item: str) -> str:
    """Removes any brackets from input string

    Parameters
    ----------
    item : str
        String to remove brackets from.

    Returns
    -------
    str
        String with brackets removed
    """
    processed_item = ''
    skip1c = 0
    skip2c = 0
    for char in item:
        if char == '[':
            skip1c += 1
        elif char == '(':
            skip2c += 1
        elif char == ']' and skip1c > 0:
            skip1c -= 1
        elif char == ')'and skip2c > 0:
            skip2c -= 1
        elif skip1c == 0 and skip2c == 0:
            processed_item += char
    return processed_item


menu_items_to_remove = [
    "cup","way","sol","uni","can","mix","hot","mac","red","hat","nem","pop",
    "nan","res","the","cafe","inch","thin","soda","cake","bowl","tune","live",
    "mild","club","cola","lime","beer","sole","well","solo","coka","fire",
    "roll","dark","wine","chef","sake","diet","soup","fool","pils","coke",
    "pick","sides","super","spicy","large","order","unity","pique","sides",
    "small","juice","combo","coffe","toast","limes","liver","lemon","sauce",
    "fried","green","limca","fruit","jumbo","meats","cocoa","basic","pound",
    "plate","coast","drink","black","white","house","water","plain","large",
    "lunch","sunny","truly","pepsi","baked","chips","crush","banks","fanta",
    "shake","royal","garden","powers","crusts","virtue","waters","people",
    "single","friday","labneh","uptown","liters","juices","corona","crimes",
    "robust","tender","pieces","pizzas","salumi","loaded","sunset","scoops",
    "gloves","sunday","medium","coffee","farmer","parlor","clever","donpx,",
    "sprite","extras","simple","heater","taste","makers","bottle","drinks",
    "deluxe","unique","chef's","lunch a","lunch b","lunch c","lunch d",
    "lunch e","lunch f","lunch g","lunch h","lunch i","lunch j","lunch k",
    "lunch l","lunch m","lunch n","lunch o","lunch p","lunch q","lunch r",
    "lunch s","lunch t","lunch u","lunch v","lunch w","lunch x","lunch y",
    "lunch z","pop ups","buffalo","napkins","chopped","phoenix","cluster",
    "patriot","one egg","the egg","ketchup","baskets","genesis","average",
    "v juice","chamber","or less","two egg","absolut","chronic","biscuit",
    "imports","degrees","supreme","century","mondays","regular","special",
    "doubles","t shirt","classic","awesome","western","original","utensils",
    "seasonal","one meat","triad in","toppings","specials","desserts",
    "can coke","thums up","original","pick two","exclusiv","can soda",
    "saturday","the kind","diabetes","sandwich","can cola","cocacola",
    "downtown","birthday","utensils","two rice","official","rotating",
    "can pops","thursday","coke can","soda pop","paradise","festival",
    "take off","tuesdays","new york","chutneys","principe","full pot",
    "manhattan","benchmark","roll of garbage bags","garbage bag each",
    "sani spritz spray","toilet paper","kids cups no spill locking lid",
    "kleenex box","sani wipes"
]


def preprocess_menu_item(item: str) -> str:
    """Pre-process menu items

    Steps applied:
        1. Remove brackets
        2. Discard items with length > 70 or < 3
        3. Lower
        4. Replace "&" with "and", "-" with " ", " w/" with " with "
        5. Remove *, ", $, #
        6. Remove oz., lb, etc
        7. Remove anyword with digits in it

    Parameters
    ----------
    item : str
        Menu item to be pre-processed
    
    Returns
    -------
    item : str
        Pre-processed menu item
    """

    # remove content of brackets and detect 0 length string
    item = remove_any_brackets(item).strip()
    len_item = len(item)
    
    if (len_item > 70) | (len_item < 3):
        # not considering menu items with length > 70 or < 3
        return ""
    else:
        item = unidecode.unidecode(item + " ")\
                                        .lower()\
                                        .replace(".", ". ")\
                                        .replace("&", "and")\
                                        .replace("-", " ")\
                                        .replace(" w/", " with ")
        to_remove = [f'\*|\"|\$|#', # remove * and " and $ and #
                     f'\d+\s*(lb|pounds|pound|oz|ounces|ounce|inches|inch'
                             f'|grams|gram|pcs|pieces|piece|each|cup'
                             f'|bowl|scoops|scoop|pot|liters|liter'
                             f'|or less|off)\s*((of)*)\.*\s+',
                     f'\s*\S*[0-9]\S*'] # remove anyword with digits in it
                     
        for pattern in to_remove:
            item = sub(pattern, ' ', item)
        
        item = ' '.join(item.replace(".", "").split())
        
        if (len(item) < 3) or (item in menu_items_to_remove):
            return ""
        else:
            return item

def get_menu(soup):
    try:
        menu_section = soup.select('.menu-sections')[0]
    except:
        # if error occurs that means the url in the db for menu is outdated
        # handle this exception in main code, and make menu_url = None
        # so, that next time the reviews scraper runs, it will update the 
        # url if exists.
        raise
    
    try:
        menu_items_dict = {}

        food_categories = menu_section.find_all("div", 
                    {"class" : "section-header section-header--no-spacing"})

        menu_items_list = menu_section.find_all("div", {"class" : "u-space-b3"})

        if len(food_categories) != len(menu_items_list):
            menu_items_list = menu_items_list[1:]

        categories_items_zip = zip(food_categories, menu_items_list)

        for food_category, menu_items in categories_items_zip:
            menu_items_dict[food_category.find('h2').text.strip().lower()] = []
            for p, price in zip(menu_items.find_all("div", {"class" : compile(r'arrange_unit arrange_unit--fill menu-item-details*')}),
                        menu_items.find_all("div", {"class" : compile(r'menu-item-prices arrange_unit*')})):
                one_category_food_items_dict = {}
                
                processed_name = None
                try:
                    name = p.find('h4').text.strip().lower()
                    processed_name = preprocess_menu_item(name)
                    if processed_name:
                        one_category_food_items_dict['name'] = name
                        one_category_food_items_dict['processed_name'] = processed_name
                        try:
                            one_category_food_items_dict['price'] = price.select_one('.menu-item-price-amount').text.replace("\\n", "").strip()
                        except:
                            pass
                        try:
                            desc = p.find('p').text
                            one_category_food_items_dict['desc'] = desc
                        except:
                            pass
                except:
                    # traceback.print_exc()
                    pass
                if processed_name:
                    menu_items_dict[food_category.find('h2').text.strip().lower()].append(one_category_food_items_dict)


        scraped_menu = {k:v for k, v in  menu_items_dict.items() if len(v)}
        
        return scraped_menu
    except:
        print_exc()
        return {}


def merge_two_dictionaries(d1: dict, d2: dict) -> dict:
    """
    Merge dictionaries `d1` and `d2` in such a way that all keys in `d2` has 
    non empty values. (if least one dict has non empty value for that key)

    If both dicts have non empty values, `d2` value will be used

    keys not in `d2` but in `d1` will directly be copied irrespective of it's value

    NOTE - by empty I mean any value that evaluates `bool(exp)` to False,
    like, "" or None or {} or []

    Example:
    d1 = {"a":'', "b":2, "d":None} # self.business_data[business_id]
    d2 = {"a":4, "b":[], "c":None} # updates dict
    merged_dict = {'a': 4, 'b': 2, 'c': None, "d":None} # all keys from `d2`
    """
    merged_dict = {}

    for k, v in d2.items():
        if not v:
            if d1.get(k):
                merged_dict[k] = d1[k]
            else:
                merged_dict[k] = v
        else:
            merged_dict[k] = v

    # not all the keys from d1 are present in d2, we need everything from d1
    for k in d1.keys():
        if k not in d2.keys():
            merged_dict[k] = d1[k]
    
    return merged_dict

class BusinessDetails:

    def __init__(self, soup):
        self.soup = soup
        self.parsed_dict_biz_updates = None
        self.parsed_dict_biz_datails = None
        self.base_key = None
        self.business_id = None
        self.is_business_closed = None # 1 or 0 or None
        self.overall_rating = None
        self._set_inst_variables(soup)
    
    def _split_camel_case(self, input_cc_string: str) -> str:
        # "ABCXyzaPqr" -> ["ABC", "Xyza", "Pqr"]
        return sub('([A-Z][a-z]+)', 
                      r' \1', 
                      sub('([A-Z]+)', r' \1', input_cc_string)).split()

    def _set_inst_variables(self, soup):
        script_json_list = soup.findAll('script', type="application/json")

        for script_json in script_json_list:
            try:
                parsed_dict = json.loads(script_json.string)
                parsed_dict_keys = parsed_dict.keys()

                if any([("$ROOT_QUERY.business" in key) 
                        for key 
                        in parsed_dict_keys]):

                    root_dict = parsed_dict.get("ROOT_QUERY")

                    for k, v in root_dict.items():
                        if "business" in k:
                            self.base_key = v.get("id")
                            self.parsed_dict_biz_updates = parsed_dict
                
                if "bizDetailsPageProps" in parsed_dict_keys:
                    try:
                        _ = (parsed_dict.get("gaConfig")
                                        .get("dimensions")
                                        .get("www"))

                        self.business_id = _.get("business_id")[1]
                        self.overall_rating = _.get("rating")[1]

                        is_business_closed = _.get("biz_closed")[1]

                        self.is_business_closed = (1 
                                                   if is_business_closed != "False" 
                                                   else 0)
                    except:
                        pass

                    self.parsed_dict_biz_datails = parsed_dict.get("bizDetailsPageProps")
            except:
                continue

    def _get_ids_list(self, dict_keys):
        sections_list_dict = (self.parsed_dict_biz_updates
                                        .get(dict_keys[0])
                                        .get(dict_keys[1]))

        if not isinstance(sections_list_dict, list):
            sections_list_dict = [sections_list_dict]

        sections_ids_list = [section_dict.get("id") 
                             for section_dict 
                             in sections_list_dict]

        attribute_ids_list = []
        for section_id in sections_ids_list:
            attribute_ids_list_dict = (self.parsed_dict_biz_updates
                                                .get(section_id)
                                                .get(dict_keys[2]))

            attribute_ids_list.extend([attribute.get("id") 
                                        for attribute 
                                        in attribute_ids_list_dict])

        return attribute_ids_list

    def get_covid19_updates(self):
        covid19_updates = {}

        if self.base_key:
            try:
                key1 = ".".join([self.base_key, "serviceUpdateSummary"])
                key2 = "attributeAvailabilitySections"
                key3 = "attributeAvailabilityList"

                attribute_ids_list = self._get_ids_list((key1, key2, key3))

                for attribute_id in attribute_ids_list:
                    label = (self.parsed_dict_biz_updates
                                    .get(attribute_id)
                                    .get("label"))

                    label = label.lower().split()
                    label = "_".join(["covid19"] + label) # covid19_label
                    label = label.replace("-", "_")

                    availability = (self.parsed_dict_biz_updates
                                            .get(attribute_id)
                                            .get("availability"))

                    covid19_updates[label] = (1
                                              if availability == "AVAILABLE" 
                                              else 0)
            except:
                return covid19_updates
        
        return covid19_updates

    def get_amenities(self):
        amenities = {}

        if self.base_key:
            try:
                key1 = self.base_key
                key2 = "organizedProperties({\"clientPlatform\":\"WWW\"})"
                key3 = "properties"

                amenities_ids_list = self._get_ids_list((key1, key2, key3))

                for amenities_id in amenities_ids_list:
                    amenity = (self.parsed_dict_biz_updates
                                        .get(amenities_id)
                                        .get("alias"))

                    amenity = self._split_camel_case(amenity) # list
                    amenity = "_".join(["amenity"] + amenity) # amenity_label
                    amenity = amenity.lower().replace("-", "_")

                    is_active = (self.parsed_dict_biz_updates
                                        .get(amenities_id)
                                        .get("isActive"))

                    amenities[amenity] = 1 if is_active else 0
            except:
                return amenities
        
        return amenities

    def get_operation_hours(self):
        operation_hours = { 'operation_hours_mon': None,
                            'operation_hours_tue': None,
                            'operation_hours_wed': None,
                            'operation_hours_thu': None,
                            'operation_hours_fri': None,
                            'operation_hours_sat': None,
                            'operation_hours_sun': None }

        if self.base_key:
            try:
                key1 = self.base_key
                key2 = "operationHours"
                key3 = "regularHoursMergedWithSpecialHoursForCurrentWeek"

                op_hours_ids_list = self._get_ids_list((key1, key2, key3))

                for op_hours_id in op_hours_ids_list:
                    day_of_week = (self.parsed_dict_biz_updates
                                            .get(op_hours_id)
                                            .get("dayOfWeekShort").lower())

                    hours = (self.parsed_dict_biz_updates.get(op_hours_id)
                                        .get("regularHours")
                                        .get("json")[0].lower())

                    operation_hours["_".join(["operation", "hours", day_of_week])] = hours
            except:
                print_exc()
                return operation_hours
        
        return operation_hours

    def get_categories(self):
        categories = []

        try:
            categories_ids_list_dict = (self.parsed_dict_biz_updates
                                                .get(self.base_key)
                                                .get("categories"))

            categories_ids_list = [category_id_dict.get("id")
                                    for category_id_dict
                                    in categories_ids_list_dict]

            for category_id in categories_ids_list:
                categories.append(self.parsed_dict_biz_updates\
                                            .get(category_id)\
                                            .get("title"))
        except:
            pass
            
        return categories

    def get_price_range(self):
        price_range = None
        try:
            price_range = (self.parsed_dict_biz_updates
                                    .get(".".join([self.base_key, 
                                                   "priceRange"]))
                                    .get("description"))
        except:
            pass

        return price_range 

    def get_phone_number(self):
        phone_number = None
        try:
            phone_number = (self.parsed_dict_biz_updates
                                    .get(".".join([self.base_key, 
                                                   "phoneNumber"]))
                                    .get("formatted"))
        except:
            pass

        return phone_number
    
    def get_address(self):
        address = { 'address_line1': None,
                    'address_line2': None,
                    'address_line3': None,
                    'city': None,
                    'region_code': None,
                    'postal_code': None,
                    'country_code': None }
        try:
            address_ = (self.parsed_dict_biz_updates
                                .get(".".join([self.base_key, 
                                               "location", 
                                               "address"])))

            address_ = {"_".join(self._split_camel_case(k)).lower(): (v if v else "") 
                        for k, v 
                        in address_.items()}

            country_code = (self.parsed_dict_biz_updates
                                    .get(".".join([self.base_key, 
                                                   "location", 
                                                   "country"]))
                                    .get("code"))

            address_ = {**address_, **{"country_code" : country_code}}

            for key in address.keys():
                address[key] = address_.get(key)
        except:
            pass

        return address


    def get_year_established(self):
        # Year of establishment
        try:
            year_established = (self.parsed_dict_biz_datails
                                        .get("fromTheBusinessProps")
                                        .get("fromTheBusinessContentProps")
                                        .get("yearEstablished"))

            year_established = int(year_established)
        except:
            year_established = None
        
        return year_established

    def get_top_food_items(self):
        # Top menu items (list)
        try:
            top_food_items_dict_list = (self.parsed_dict_biz_datails
                                            .get("popularDishesCarouselProps")
                                            .get("popularDishes"))

            top_food_items = [preprocess_menu_item(i["dishName"]) 
                              for i 
                              in top_food_items_dict_list]
        except:
            top_food_items = []
        
        return top_food_items

    def get_menu_url(self):
        # Menu URL (URL or None)
        menu_url = None
        try:
            if not (self.parsed_dict_biz_datails
                            .get("bizContactInfoProps")
                            .get("businessMenuProps")
                            .get("isExternalMenu")):

                menu_url = (self.parsed_dict_biz_datails
                                    .get("bizContactInfoProps")
                                    .get("businessMenuProps")
                                    .get("menuLink")
                                    .get("href"))
        except:
            try:
                print(f"trying to find menu url in soup..")
                full_menu = self.soup.find(text='Yelp menu')
                menu_url = full_menu.find_parent("a", href=True)["href"].split("www.yelp.com")[-1]
                menu_url = menu_url if "/biz_redir?" not in menu_url else None
            except:
                pass

        return menu_url
    
    def get_num_reviews(self):
        # Number of reviewws
        try:
            num_reviews = (self.parsed_dict_biz_datails
                                    .get("ratingDetailsProps")
                                    .get("numReviews"))
        except:
            print(f"trying to find num_reviews in soup..")
            try:
                num_reviews = int(self.soup.find(text = compile(r"\d+ reviews")).split(' ')[0])
            except:
                num_reviews = None
        
        return num_reviews

    def get_monthly_ratings_by_year(self):
        # Monthly ratings by year
        try:
            monthly_ratings_by_year = (self.parsed_dict_biz_datails
                                                .get("ratingDetailsProps")
                                                .get("monthlyRatingsByYear"))
        except:
            monthly_ratings_by_year = {}
        
        return monthly_ratings_by_year

    def get_rating_histogram(self):
        # Rating histogram
        rating_histogram = {'num_reviews_5_stars': None,
                            'num_reviews_4_stars': None,
                            'num_reviews_3_stars': None,
                            'num_reviews_2_stars': None,
                            'num_reviews_1_star': None}
        try:
            rating_histogram_list = (self.parsed_dict_biz_datails
                                            .get("ratingDetailsProps")
                                            .get("ratingHistogram")
                                            .get("histogramData"))

            for rating_dict in rating_histogram_list:
                num_reviews = rating_dict.get("count")
                label = rating_dict.get("label")
                label = "_".join(["num", "reviews"] + label.split())
                rating_histogram[label] = num_reviews
        except:
            pass
        
        return rating_histogram

    def get_all_biz_details(self):
        details = {}
        details["is_business_closed"] = self.is_business_closed
        details["overall_rating"] = self.overall_rating
        details["year_established"] = self.get_year_established()
        details["num_reviews"] = self.get_num_reviews()
        details["menu_url"] = self.get_menu_url()
        details["price_range"] = self.get_price_range()
        details["phone_number"] = self.get_phone_number()

        address = self.get_address()
        details = {**details, **address}

        operation_hours = self.get_operation_hours()
        details = {**details, **operation_hours}

        details["categories"] = self.get_categories()
        details["top_food_items"] = self.get_top_food_items()
        details["monthly_ratings_by_year"] = self.get_monthly_ratings_by_year()

        rating_histogram = self.get_rating_histogram()
        details = {**details, **rating_histogram}

        return details

    def get_all_updates_and_details(self):
        business_id = self.business_id
        covid19_updates = self.get_covid19_updates()
        amenities = self.get_amenities()

        return {"business_id" : business_id,
                "business_details" : self.get_all_biz_details(),
                "covid19_updates" : covid19_updates,
                "amenities" : amenities}