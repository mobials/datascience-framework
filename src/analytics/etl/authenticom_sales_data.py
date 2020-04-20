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
import csv
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
with postgreshandler.get_analytics_connection() as connection:
    for file in postgreshandler.get_s3_completed_files(connection,script):
        s3_completed_files.append(file)
    s3_completed_files = set(s3_completed_files)

bucket = src.settings.s3_authenticom

resource = boto3.resource('s3')

objects = resource.Bucket(bucket).objects.all()

for object_summary in objects:
    last_modified = object_summary.last_modified
    print(last_modified)
    key = object_summary.key
    if not key.startswith('sales/'):
        continue
    file = bucket + '/' + key
    if file in s3_completed_files:
        continue
    tuples = []

    object = resource.Object(
        bucket_name=bucket,
        key=key
    )

    iterator = object.get()['Body'].iter_lines()
    with postgreshandler.get_analytics_connection() as connection:
        s3_id = postgreshandler.insert_s3_file(connection, script, file, last_modified)
        column_names = None
        for line in iterator:
            text = line.decode()
            column_values = ['{}'.format(x) for x in list(csv.reader([text], delimiter='\t',quotechar='"'))[0]]
            if not column_names:
                column_names = column_values
            else:
                info = dict(zip(column_names, column_values))

                tuple = (
                    s3_id,
                    psycopg2.extras.Json(info),
                )

                tuples.append(tuple)

        if len(tuples) > 0:
            with connection.cursor() as cursor:
                psycopg2.extras.execute_values(cursor, insert_query, tuples)


