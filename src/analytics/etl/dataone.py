import sys
sys.path.insert(0,'../..')
sys.path.append('/var/www/datascience-framework/src/')
import psycopg2
import postgreshandler
import os
import boto3
import settings
import datetime
import pytz
import gzip
import csv
import psycopg2.extras
import time
import utility
import re

schema = 'vendors'
script = os.path.basename(__file__)[:-3]

insert_query =  '''
                    INSERT INTO 
                        {0}.{1}
                    (
                        s3_id,
                        vin_pattern,
                        vehicle_id,
                        payload
                    )
                    VALUES 
                        %s
                '''.format(schema,script)

truncate_query =    '''
                        TRUNCATE TABLE {0}.{1};
                    '''.format(schema,script)


while True:
    schedule_info = None
    scheduler_connection = postgreshandler.get_analytics_connection()
    schedule_info = postgreshandler.get_script_schedule(scheduler_connection,schema,script)
    if schedule_info is None:
        raise Exception('Schedule not found.')
    scheduler_connection.close()

    now = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
    last_run = schedule_info['last_run']
    start_date = schedule_info['start_date']
    frequency = schedule_info['frequency']
    status = schedule_info['status']
    last_update = schedule_info['last_update']
    run_time = schedule_info['run_time']
    next_run = None
    if last_run is None:
        next_run = start_date
    else:
        next_run = utility.get_next_run(start_date, last_run, frequency)

    if now < next_run:
        seconds_between_now_and_next_run = (next_run - now).seconds
        time.sleep(seconds_between_now_and_next_run)
        continue  # continue here becuase it forces a second check on the scheduler, which may have changed during the time the script was asleep

    start_time = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)

    etl_connection = postgreshandler.get_analytics_connection()
    try:
        last_modified = postgreshandler.get_s3_scanned_max_last_modified_date(etl_connection, script)
        if last_modified is None:
            last_modified = datetime.datetime(2000, 1, 1).replace(tzinfo=pytz.utc)

        bucket = settings.s3_dataone_bucket

        key = settings.s3_dataone_key

        resource = boto3.resource('s3')

        object = resource.Object(bucket, key)

        file = bucket + '/' + key + '/' + object.version_id

        if object.last_modified > last_modified:
            last_modified = object.last_modified
            body = object.get()['Body']
            with gzip.GzipFile(fileobj=body) as gzipfile:
                s3_id = postgreshandler.insert_s3_file(etl_connection,script,file,last_modified)
                tuples = []

                column_headings = None
                for line in gzipfile:
                    text = line.decode()
                    split_text = ['{}'.format(x) for x in list(csv.reader([text], delimiter='\t'))[0]]

                    if not column_headings:
                        column_headings = split_text
                        continue
                    else:
                        info = dict(zip(column_headings, split_text))

                        vin_pattern = info['VIN_PATTERN']
                        vehicle_id = int(info['VEHICLE_ID'])

                        del info['VIN_PATTERN']
                        del info['VEHICLE_ID']

                        payload = psycopg2.extras.Json(info)

                        tuple = (
                            s3_id,
                            vin_pattern,
                            vehicle_id,
                            payload
                        )

                        tuples.append(tuple)

                if len(tuples) > 0:
                    with etl_connection.cursor() as cursor:
                        cursor.execute(truncate_query)
                        psycopg2.extras.execute_values(cursor, insert_query, tuples)
                        status = 'success'
                        last_update = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
                        run_time = last_update - start_time
                etl_connection.commit()
    except Exception as e:
        status = str(e)
    finally:
        etl_connection.close()

    #update the scheduler
    scheduler_connection = postgreshandler.get_analytics_connection()
    postgreshandler.update_script_schedule(scheduler_connection,schema,script,now,status,run_time,last_update)
    scheduler_connection.commit()
    scheduler_connection.close()