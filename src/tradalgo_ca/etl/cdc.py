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
import resource

script = os.path.basename(__file__)

cdc_insert_query =  '''
                        INSERT INTO
                            cdc
                        (
                            vin,
                            miles,
                            price,
                            status_date,
                            taxonomy_vin,
                            body_type,
                            trim_orig,
                            s3_id
                        )
                        VALUES 
                            %s
                        ON CONFLICT ON CONSTRAINT cdc_vin_miles_price_pk
                        DO UPDATE 
                        SET 
                            status_date = case when cdc.status_date > EXCLUDED.status_date then cdc.status_date else EXCLUDED.status_date end,
                            s3_id = case when cdc.status_date > EXCLUDED.status_date then cdc.s3_id else EXCLUDED.s3_id end
                    '''

s3_completed_files = []
with postgreshandler.get_tradalgo_canada_connection() as connection:
    for file in postgreshandler.get_s3_completed_files(connection,script):
        s3_completed_files.append(file)
    s3_completed_files = set(s3_completed_files)

csv.field_size_limit(sys.maxsize)

s3 = boto3.resource("s3")

versions = s3.Bucket(s3_cdc_ca_bucket).object_versions.filter(Prefix=s3_cdc_ca_key)

for version in versions:
    last_modified = version.last_modified
    if last_modified < utility.add_days(datetime.datetime.utcnow().replace(tzinfo=pytz.utc),-365):
        continue
    file = s3_cdc_ca_bucket + '/' + s3_cdc_ca_key + '/' + version.version_id
    if file in s3_completed_files:
        continue
    obj = version.get()['Body']
    with gzip.GzipFile(fileobj=obj) as gzipfile:
        with postgreshandler.get_tradalgo_canada_connection() as connection:

            s3_id = postgreshandler.insert_s3_file(connection,script,file,last_modified)

            tuples = {}

            column_headings = None
            count = 0
            for line in gzipfile:
                count += 1
                text = line.decode()
                split_text = ['{}'.format(x) for x in list(csv.reader([text], delimiter=',', quotechar='"'))[0]]

                # #strip out double quotes
                # for item_index in range(len(split_text)):
                #     t = split_text[item_index]
                #     if split_text[item_index].startswith('"') and split_text[item_index].endswith('"'):
                #         split_text[item_index] = split_text[item_index][1:-1]

                if not column_headings:
                    column_headings = split_text
                    continue
                else:
                    info = dict(zip(column_headings,split_text))

                if info['miles_indicator_ss'] != 'KILOMETERS':
                    continue

                if info['currency_indicator_ss'] != 'CAD':
                    continue

                try:
                    miles = float(info['miles_fs'])
                except:
                    continue

                if miles < 1000:
                    continue
                if miles > 250000:
                    continue
                try:
                    price = float(info['price_fs'])
                except:
                    continue

                if price < 500:
                    continue

                status_date = datetime.datetime.strptime(info['status_date_dts'], "%Y-%m-%dT%H:%M:%SZ")
                vin = info['vin_ss']
                taxonomy_vin = info['taxonomy_vin_ss']
                original_body_type = info['body_type_ss'].strip()
                vehicle_type = info['vehicle_type_ss'].strip()
                body_type = utility.cdc_body_type_to_dataone(original_body_type,vehicle_type) if original_body_type is not '' else None
                trim_orig = info['trim_orig_ss'] if info['trim_orig_ss'] is not '' else None

                tuple = (
                    vin,
                    miles,
                    price,
                )

                if tuple in tuples:
                    existing_values = tuples[tuple]
                    existing_status_date = existing_values[0]
                    if existing_status_date > status_date:
                        tuples[tuple] = (status_date,taxonomy_vin,body_type,trim_orig,s3_id)
                else:
                    tuples[tuple] = (status_date,taxonomy_vin,body_type,trim_orig,s3_id)

                # if len(tuples) == 20000:
                #     break
                # print(count)

            tuples = [key + value for key,value in tuples.items()]

            if len(tuples) > 0:
                with connection.cursor() as cursor:
                    psycopg2.extras.execute_values(cursor, cdc_insert_query, tuples)

            max = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            print(max*0.000001)

#print('finished')