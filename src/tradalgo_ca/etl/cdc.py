import sys
sys.path.insert(0,'../..')
import boto3
import csv
import datetime
import psycopg2
import psycopg2.extras
import postgreshandler
import utility
import pytz
from src.settings import *
import os
import gzip

cdc_insert_query =  '''
                        INSERT INTO
                            cdc
                        (
                            s3_id,
                            status_date,
                            vin,
                            zip,
                            year,
                            make,
                            model,
                            trim,
                            body_type,
                            vehicle_type,
                            trim_orig,
                            miles,
                            price
                        )
                        VALUES 
                            %s
                        ON CONFLICT (status_date,vin,zip)
                        DO NOTHING
                    '''

csv.field_size_limit(sys.maxsize)

s3_versions = boto3.client('s3').list_object_versions(Bucket=s3_cdc_ca_bucket, Prefix=s3_cdc_ca_key)['Versions']

s3 = boto3.resource("s3")

versions = s3.Bucket(s3_cdc_ca_bucket).object_versions.filter(Prefix=s3_cdc_ca_key)

for version in versions:
    obj = version.get()['Body']
    with gzip.GzipFile(fileobj=obj) as gzipfile:


        tuples = []
        column_headings = None
        for line in gzipfile:
            text = line.decode()
            split_text = ['"{}"'.format(x) for x in list(csv.reader([text], delimiter=',', quotechar='"'))[0]]
            if not column_headings:
                column_headings = split_text
                continue
            else:
                info = dict(zip(column_headings,split_text))
            status_date = datetime.datetime.strptime(info['status_date_dts'], "%Y-%m-%dT%H:%M:%SZ")
            vin = info['vin_ss']
            zip = info['zip_is']
            year = int(info['year_is'])
            make = info['make_ss']
            model = info['model_ss']
            trim = info['trim_ss']
            body_type = info['body_type_ss']
            vehicle_type = info['vehicle_type_ss']
            city = info['city_ss']
            state = info['state_ss']
            trim_orig = info['trim_orig_ss']
            miles = float(info['miles_fs'])
            if miles < 1000:
                continue

            if miles > 250000:
                continue

            price = float(info['price_fs'])
            if price < 500:
                continue

            tuppe = (
                        s3_id,
                        status_date,
                        vin,
                        zip,
                        year,
                        make,
                        model,
                        trim,
                        body_type,
                        vehicle_type,
                        city,
                        state,
                        trim_orig,
                        miles,
                        price,
                     )

            tuples.append(tuple)


print('finished')