import sys
sys.path.insert(0,'../..')
#sys.path.append('/Users/caseywood/Documents/GitHub/analytics-framework/src/utility.py')
import boto3
import src.settings
import io
import zipfile
import postgreshandler
import json
import os
import datetime
import psycopg2
import psycopg2.extras
import utility
import pytz

avr_widget_impressions_daily_insert_query =  '''
                    INSERT INTO
                        avr_widget_impressions_daily
                    (
                        s3_id,
                        date,
                        master_business_id,
                        integration_settings_id,
                        ip_address,
                        product,
                        device_type
                    )
                    VALUES
                        %s
                    ON CONFLICT ON CONSTRAINT avr_widget_impressions_daily_pk
                    DO NOTHING
                '''

script = os.path.basename(__file__)

s3_completed_files = []
with postgreshandler.get_analytics_connection() as connection:
    for file in postgreshandler.get_s3_completed_files(connection,script):
        s3_completed_files.append(file)
    s3_completed_files = set(s3_completed_files)

bucket = src.settings.s3_firehose_bucket_production

#client = boto3.client('s3')

resource = boto3.resource('s3')

objects = resource.Bucket(bucket).objects.all()

for object_summary in objects:
    last_modified = object_summary.last_modified
    #if last_modified < datetime.datetime(2020,1,29).replace(tzinfo = pytz.utc):
    #    continue
    print(last_modified)
    key = object_summary.key
    file = bucket + '/' + key
    if file in s3_completed_files:
        continue
    tuples = []

    object = resource.Object(
        bucket_name=bucket,
        key=key
    )
    buffer = io.BytesIO(object.get()["Body"].read())
    z = zipfile.ZipFile(buffer)
    with  z.open(z.infolist()[0]) as f:
        with postgreshandler.get_analytics_connection() as connection:
            s3_id = postgreshandler.insert_s3_file(connection, script, file, last_modified)
            for line in f:
                info = json.loads(line)
                happened_at = datetime.datetime.strptime(info['happened_at'], "%Y-%m-%dT%H:%M:%S+00:00")
                date = utility.get_day(happened_at)
                master_business_id = info['master_business_id']
                integration_settings_id = info['integration_settings_id']
                ip_address = info['ip_address']
                product = info['product']
                device_type = info['deviceType'] if 'deviceType' in info else 'unknown'
                tuple = (
                            s3_id,
                            date,
                            master_business_id,
                            integration_settings_id,
                            ip_address,
                            product,
                            device_type,
                )

                tuples.append(tuple)
    tuples = set(tuples)
    if len(tuples) > 0:
        with postgreshandler.get_datascience_connection() as connection:
            with connection.cursor() as cursor:
                psycopg2.extras.execute_values(cursor,avr_widget_impressions_daily_insert_query,tuples)


