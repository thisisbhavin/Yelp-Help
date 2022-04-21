from scrapy import Field
from scrapy import Item

class Review(Item):
    review_id = Field()
    review = Field()
    date = Field()
    rating = Field()
    business_name = Field()
    business_id = Field()
    business_location = Field()
    business_alias = Field()
    sentiment = Field()

class Business(Item):
    business_id = Field()
    business_name = Field()
    # psudo_location = Field()
    # query_name = Field()
    # matched_score = Field()
    overall_rating = Field()
    business_url = Field()
    num_reviews = Field()
    location = Field()
    categories = Field()
    phone_number = Field()
    # address_line1 = Field()