"""Create Restaurants Info table

Revision ID: 9335f701f671
Revises: 
Create Date: 2021-11-02 17:18:52.508390

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9335f701f671'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute('''
        DROP TABLE IF EXISTS restaurants_info;

        CREATE TABLE restaurants_info (
            business_id text NOT NULL,
            business_name text NULL,
            business_url text NOT NULL,
            "location" text NULL, -- The location value you put in yelp's search box to get all businesses in that location
            is_business_closed int2 NULL,
            year_established int8 NULL,
            overall_rating float4 NULL,
            num_reviews int2 NULL, -- This is the number of reviews as on last run.  This might not be the latest number of reviews
            menu_url text NULL,
            menu_items_scraped_flag int4 NULL DEFAULT 0, -- 1 - menu items has been scraped, 0 - menu items need to be scraped (if available)
            price_range text NULL,
            phone_number text NULL,
            num_reviews_5_stars int2 NULL,
            num_reviews_4_stars int2 NULL,
            num_reviews_3_stars int2 NULL,
            num_reviews_2_stars int2 NULL,
            num_reviews_1_star int2 NULL,
            address_line1 text NULL,
            address_line2 text NULL,
            address_line3 text NULL,
            city text NULL,
            region_code bpchar(2) NULL,
            postal_code int4 NULL,
            country_code bpchar(2) NULL,
            operation_hours_mon text NULL,
            operation_hours_tue text NULL,
            operation_hours_wed text NULL,
            operation_hours_thu text NULL,
            operation_hours_fri text NULL,
            operation_hours_sat text NULL,
            operation_hours_sun text NULL,
            categories _text NULL,
            top_food_items _text NULL, -- Top food items as on webpage of a particular restaurant
            monthly_ratings_by_year jsonb NULL, -- { year1 : [[month_index, rating], [month_index, rating], ..], year2 : [...], ...}
            last_reviews_count int2 NOT NULL DEFAULT '-1'::integer, -- Number of reviews as on last review scraper run. if -1 then all reviews need to be scraped
            errors_at text NOT NULL DEFAULT '-1'::text, -- list of ranges of reviews to scrape. if -1 and last_review_count != -1 then all reviews scraped
            menu jsonb NULL,
            CONSTRAINT restaurants_info_pk PRIMARY KEY (business_id)
        );

        COMMENT ON COLUMN restaurants_info."location" IS 'The location value you put in yelp''s search box to get all businesses in that location';
        COMMENT ON COLUMN restaurants_info.num_reviews IS 'This is the number of reviews as on last run.  This might not be the latest number of reviews';
        COMMENT ON COLUMN restaurants_info.menu_items_scraped_flag IS '1 - menu items has been scraped, 0 - menu items need to be scraped (if available)';
        COMMENT ON COLUMN restaurants_info.top_food_items IS 'Top food items as on webpage of a particular restaurant';
        COMMENT ON COLUMN restaurants_info.monthly_ratings_by_year IS '{ year1 : [[month_index, rating], [month_index, rating], ..], year2 : [...], ...}';
        COMMENT ON COLUMN restaurants_info.last_reviews_count IS 'Number of reviews as on last review scraper run. if -1 then all reviews need to be scraped';
        COMMENT ON COLUMN restaurants_info.errors_at IS 'list of ranges of reviews to scrape. if -1 and last_review_count != -1 then all reviews scraped';


    ''') # noqa


def downgrade():
    op.execute('''
        DROP TABLE IF EXISTS restaurants_info;
    ''')
