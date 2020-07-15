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

#import sage customer info
bucket = 'datascience-framework-auxiliary-data'

key = 'SQL - Customer Info (20200713).csv'

resource = boto3.resource('s3')

object = resource.Object(
                bucket_name=bucket,
                key=key
            )
response = object.get()


for row in csv.DictReader(codecs.getreader('utf-8')(response[u'Body'])):


    print(row)






