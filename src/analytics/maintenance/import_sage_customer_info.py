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

customer_info_query =   '''
                            INSERT INTO 
                                sage.customer_info
                            (
                                id,
                                payload 
                            )
                            VALUES
                            (
                                %(id)s,
                                %(payload)s
                            )
                            ON CONFLICT (id)
                            DO NOTHING;
                        '''



#import sage customer info
bucket = 'datascience-framework-auxiliary-data'

key = 'SQL - Customer Info (20200713).csv'

resource = boto3.resource('s3')

object = resource.Object(
                bucket_name=bucket,
                key=key
            )
response = object.get()


with postgreshandler.get_analytics_connection() as connection:
    with connection.cursor() as cursor:
        for row in csv.DictReader(codecs.getreader('utf-8')(response[u'Body'])):
            id = int(row['lId'])
            del row['lId']
            payload = json.dumps(row, default=str)
            cursor.execute(customer_info_query,{'id':id,'payload':payload})
            print(id)

print('finished')






