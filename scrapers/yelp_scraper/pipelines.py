import gzip
from datetime import datetime
from io import BytesIO
from traceback import print_exc

from google.cloud import storage
import psycopg2
from psycopg2.extras import execute_values

from scrapy.exporters import JsonLinesItemExporter
from scrapy.utils.project import get_project_settings

from yelp_scraper.credentials import GCP
from yelp_scraper.credentials import Postgres

settings = get_project_settings()


class PostgresWriterPipeline:
    """
    Pipeline to store scraped business information or reviews to PostgreSQl
    database
    """
    def __init__(self):
        super(PostgresWriterPipeline, self).__init__()
        self.items_buffer = []
        self.conn = None
        self.cursor = None

    def get_db_connection(self):
        return psycopg2.connect(host = Postgres.PG_HOST,
                                port = Postgres.PG_PORT,
                                dbname = Postgres.PG_DBNAME,
                                user = Postgres.PG_USER,
                                password = Postgres.PG_PWD)
        

    def upload_to_db(self, spider_name : str) -> None:
        try:
            self.conn = self.get_db_connection()
            self.cursor = self.conn.cursor()

            if spider_name in ['Businesses', "Businesses_by_Name"]:
                # The business can be scraped twice - different filter could
                # give same business - so, removing those duplicates.
                seen = set()
                data_to_upload = []
                for item_dict in self.items_buffer:
                    if item_dict["business_id"] in seen:
                        pass
                    else:
                        data_to_upload.append(item_dict)
                        seen.add(item_dict["business_id"])

                print(f"Num of scraped items - {len(data_to_upload)}")
                # print(f"DO UPDATE SET {', '.join([''.join([str(k), ' = excluded.', str(k)]) for k in data_to_upload[0].keys() if k not in ['location', 'psudo_location', 'query_name']])}")
                columns_of_data = [k for k in data_to_upload[0].keys() if k not in ['psudo_location', 'query_name']]
                execute_values(self.cursor, 
                                (f"INSERT INTO {Postgres.PG_TABLE_NAME} ({', '.join(columns_of_data)}) "
                                 f"VALUES %s "
                                 f"ON CONFLICT (business_id) "
                                 f"DO UPDATE SET {', '.join([''.join([str(k), ' = excluded.', str(k)]) for k in data_to_upload[0].keys() if k not in ['location', 'psudo_location', 'query_name']])}"),
                               [[value for key, value in item_dict.items() if key not in ["psudo_location", "query_name"]] for item_dict in data_to_upload])

            self.items_buffer = []
        except:
            print_exc()
            print('exception') 
        finally:
            if self.conn:
                self.conn.commit()
                self.cursor.close()
                self.conn.close()

    def process_item(self, item, spider):
        # Converting item dict to tuple for bulk upload to db
        # this will break if order in insert query is not same as order in dict
        self.items_buffer.append(item)
            
        if len(self.items_buffer) >= 1000:
            print("inserting scraped items to db")
            self.upload_to_db(spider.name)

        return item

    def close_spider(self, spider):
        if self.items_buffer:
            print("inserting scraped items to db")
            self.upload_to_db(spider.name)


class CSPipeline:
    """
    Pipeline to save scraped items to a GCP bucket in gzipped json lines files
    - Upload data in chunks (default chunk size = 1000)
    - Save files in bucket with name folder name being name of location
    """

    def __init__(self):
        self.bucket_name = GCP.BUCKET
        self.object_key_template = GCP.FILE_URL

        self.max_chunk_size = settings.getint('BUCKET_MAX_CHUNK_SIZE', 1000)
        self.use_gzip = settings.getbool('WANT_TO_GZIP', 
                                         self.object_key_template.endswith('.gz'))

        self.storage_client = storage.Client()
        self.items = []
        self.chunk_number = 0

    def process_item(self, item, spider):
        self.items.append(item)
        self.city = item.get('business_location')
        if len(self.items) >= self.max_chunk_size:
            self._upload_chunk(spider)

        return item

    def close_spider(self, spider):
        # Upload remained items to CS.
        self._upload_chunk(spider)

    def _upload_chunk(self, spider):
        if not self.items:
            print('No reviews scraped!')
            return  # Do nothing when items is empty.

        f = self._make_fileobj()

        # Build object key by replacing variables in object key template.
        object_key = self.object_key_template.format(**self._get_uri_params(spider))
        print(object_key)

        try:
            # assuming bucket exists
            bucket = self.storage_client.get_bucket(self.bucket_name)
            blob = bucket.blob(object_key)
            blob.upload_from_file(f)
            
        except:
            print('pipeline CS fail')
            raise
        else:
            print('pipeline CS success')
        finally:
            # Prepare for the next chunk
            self.chunk_number += len(self.items)
            self.items = []

    def _get_uri_params(self, spider):
        params = {}
        params['city'] = self.city
        params['chunk'] = self.chunk_number
        params['time'] = datetime.utcnow().replace(microsecond=0) \
                                          .isoformat() \
                                          .replace(':', '-')
        return params

    def _make_fileobj(self):
        fileobj = BytesIO()
        f = gzip.GzipFile(mode='wb', fileobj=fileobj) if self.use_gzip else fileobj

        # Build file object using ItemExporter
        exporter = JsonLinesItemExporter(f)
        exporter.start_exporting()
        for item in self.items:
            exporter.export_item(item)
        exporter.finish_exporting()

        if f is not fileobj:
            f.close()  # Close the file if GzipFile

        # Seek to the top of file
        fileobj.seek(0)

        return fileobj
