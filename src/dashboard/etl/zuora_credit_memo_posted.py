import sys
sys.path.insert(0,'../..')
import boto3
import src.settings
import s3streaming
import io
import zipfile
import sys
import json
import postgreshandler
import os
import datetime
import psycopg2
import psycopg2.extras
import pytz

script = os.path.basename(__file__)[:-3]

insert_query =  '''
                    INSERT INTO 
                        {0}
                    (
                        s3_id,
                        payload
                    )
                    VALUES 
                        %s
                '''.format(script)

s3_completed_files = []
with postgreshandler.get_dashboard_connection() as connection:
    for file in postgreshandler.get_s3_completed_files(connection,script):
        s3_completed_files.append(file)
    s3_completed_files = set(s3_completed_files)

bucket = src.settings.s3_firehose_bucket

resource = boto3.resource('s3')

objects = resource.Bucket(bucket).objects.all()

for object_summary in objects:
    last_modified = object_summary.last_modified
    if last_modified < datetime.datetime(2020,4,1).replace(tzinfo=pytz.utc):
        continue
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
        with postgreshandler.get_dashboard_connection() as connection:
            s3_id = postgreshandler.insert_s3_file(connection, script, file, last_modified)
            for line in f:
                info = json.loads(line)
                if 'event_name' not in info:
                    continue
                if info['event_name'] != 'zuora.invoice_item.created':
                    continue

                tuple = (
                    s3_id,
                    psycopg2.extras.Json(info),
                )

                tuples.append(tuple)

            if len(tuples) > 0:
                with connection.cursor() as cursor:
                    psycopg2.extras.execute_values(cursor, zuora_invoice_item_created_insert_query, tuples)

