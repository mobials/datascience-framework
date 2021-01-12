import sys
sys.path.insert(0,'../..')
sys.path.append('/var/www/datascience-framework/src/')
import boto3
import settings
import io
import zipfile
import json
import postgreshandler
import os
import datetime
import psycopg2
import psycopg2.extras
import pytz
import time
import utility
import csv
import codecs
import gzip

is_gzip = True
schema = 'scratch'
table = 'mc_us_used'

csv.field_size_limit(sys.maxsize)

bucket = settings.s3_cdc_us_bucket
key = settings.s3_cdc_us_key

create_table_query =    '''
                            CREATE TABLE IF NOT EXISTS
                                {0}.{1}
                            (
                                payload json
                            );
                            TRUNCATE TABLE {0}.{1};
                        '''.format(schema,table)

insert_query =  '''
                    INSERT INTO 
                        {0}.{1}
                    (
                        payload 
                    )
                    VALUES
                        %s
                '''.format(schema,table)

resource = boto3.resource('s3')
object = resource.Object(
    bucket_name=bucket,
    key=key
)
response = object.get()['Body']

with postgreshandler.get_analytics_connection() as connection:
    with connection.cursor() as cursor:
        cursor.execute(create_table_query)
        tuples = []
        if is_gzip:
            with gzip.GzipFile(fileobj=response) as gzipfile:
                column_headings = None
                count = 0
                for line in gzipfile:
                    count += 1
                    print(count)
                    text = line.decode()
                    split_text = ['{}'.format(x) for x in list(csv.reader([text], delimiter=',', quotechar='"'))[0]]

                    if not column_headings:
                        column_headings = split_text
                        continue
                    else:
                        info = dict(zip(column_headings, split_text))

                        if 'photo_links_ss' in info:
                            del info['photo_links_ss']

                        if 'seller_comments_texts' in info:
                            del info['seller_comments_texts']
                        if 'options_texts' in info:
                            del info['options_texts']

                        if 'features_texts' in info:
                            del info['features_texts']

                        payload = json.dumps(info, default=str)

                        tuple = (payload,)
                        tuples.append(tuple)

        if len(tuples) > 0:
            print('uploading')
            psycopg2.extras.execute_values(cursor, insert_query, tuples)

print('finished')






