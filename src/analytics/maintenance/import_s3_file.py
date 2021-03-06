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

columns_to_keep = [
    'dealer_id_is',
    'source_ss',
    'zip_is',
    'address_ss',
    'city_ss',
    'state_ss',
    'street_ss',
    'seller_email_ss',
    'seller_phone_ss',
    'seller_name_orig_ss'
]

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

def clean_info(info,columns_to_keep):
    if len(columns_to_keep) > 0:
        result = {}
        for column in columns_to_keep:
            if column in info:
                result[column] = info[column]
        return result
    return info

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
                        info = clean_info(info,columns_to_keep)

                        payload = json.dumps(info, default=str)

                        tuple = (payload,)
                        tuples.append(tuple)

        if len(tuples) > 0:
            print('uploading')
            psycopg2.extras.execute_values(cursor, insert_query, tuples)

print('finished')






