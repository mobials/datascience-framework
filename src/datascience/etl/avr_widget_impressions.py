import sys
sys.path.insert(0,'../..')
#sys.path.append('/Users/caseywood/Documents/GitHub/datascience-framework/src/utility.py')
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

insert_query =  '''
                    INSERT INTO
                        avr_widget_impressions
                    (
                        event_id,
                        happened_at,
                        master_business_id,
                        integration_settings_id,
                        ip_address,
                        product,
                        device_type
                    )
                    VALUES
                        %s
                    RETURNING 1
                '''

script = os.path.basename(__file__)

s3_completed_files = []
with postgreshandler.get_datascience_connection() as connection:
    for file in postgreshandler.get_s3_completed_files(connection,script):
        s3_completed_files.append(file)

bucket = src.settings.s3_firehose_bucket_production

client = boto3.client('s3')

resource = boto3.resource('s3')

for object_info in client.list_objects(Bucket=bucket)['Contents']:
    key = object_info['Key']
    if key in s3_completed_files:
        continue

    tuples = []

    object = resource.Object(
        bucket_name=bucket,
        key=key
    )
    buffer = io.BytesIO(object.get()["Body"].read())
    z = zipfile.ZipFile(buffer)
    with  z.open(z.infolist()[0]) as f:
        reads = 0
        for line in f:
            reads += 1
            info = json.loads(line)

            event_id = info['event_id']
            happened_at = datetime.datetime.strptime(info['happened_at'], "%Y-%m-%dT%H:%M:%S+00:00")
            master_business_id = info['master_business_id']
            integration_settings_id = info['integration_settings_id']
            ip_address = info['ip_address']
            product = info['product']
            device_type = info['deviceType'] if 'deviceType' in info else None
            tuple = (
                        event_id,
                        happened_at,
                        master_business_id,
                        integration_settings_id,
                        ip_address,
                        product,device_type,
            )

            tuples.append(tuple)
            #print(reads)

    if len(tuples) > 0:
        with postgreshandler.get_datascience_connection() as connection:
            with connection.cursor() as cursor:
                psycopg2.extras.execute_values(cursor,insert_query,tuples)


