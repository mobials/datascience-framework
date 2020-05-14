import sys
sys.path.insert(0,'../..')
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

import time

script = os.path.basename(__file__)[:-3]

insert_query =  '''
                    INSERT INTO
                        event_name_key_dates
                    (
                        s3_id,
                        event_name,
                        first_date,
                        last_date
                    )
                    VALUES
                        %s
                    ON CONFLICT ON CONSTRAINT event_name_key_dates_pk
                    DO NOTHING
                '''

s3_completed_files = []
with postgreshandler.get_analytics_connection() as connection:
    for file in postgreshandler.get_s3_scanned_files(connection,script):
        s3_completed_files.append(file)
    s3_completed_files = set(s3_completed_files)

bucket = src.settings.s3_firehose_bucket_production

resource = boto3.resource('s3')

objects = resource.Bucket(bucket).objects.all()

for object_summary in objects:
    last_modified = object_summary.last_modified
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
            info = {}
            s3_id = postgreshandler.insert_s3_file(connection, script, file, last_modified)
            for line in f:
                file_key_dates = json.loads(line)
                if'event_name' not in info:
                    continue
                event_name = info['event_name']
                first_date = None
                last_date = None
                #if event_name == 'avr.widget.impression':



    #             tuples.append(tuple)
    # tuples = set(tuples)
    #
    # if len(tuples) > 0:
    #     with postgreshandler.get_datascience_connection() as connection:
    #         with connection.cursor() as cursor:
    #             psycopg2.extras.execute_values(cursor,insert_query,tuples)

