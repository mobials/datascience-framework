import sys
sys.path.insert(0,'../..')
import boto3
import settings
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
                        {0}
                    (
                        s3_id,
                        date,
                        master_business_id,
                        integration_settings_id,
                        ip_address,
                        product,
                        device_type,
                        referrer_url
                    )
                    VALUES
                        %s
                    ON CONFLICT ON CONSTRAINT {0}_pk
                    DO NOTHING
                '''.format(script)

while True:
    schedule_info = None
    scheduler_connection = postgreshandler.get_analytics_connection()
    schedule_info = postgreshandler.get_script_schedule(scheduler_connection, script)
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
        s3_completed_files = []
        for file in postgreshandler.get_s3_scanned_files(etl_connection, script):
            s3_completed_files.append(file)
        s3_completed_files = set(s3_completed_files)

        bucket = settings.s3_firehose_bucket_production

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
                s3_id = postgreshandler.insert_s3_file(etl_connection, script, file, last_modified)
                for line in f:
                    info = json.loads(line)
                    if 'event_name' not in info:
                        continue
                    if info['event_name'] != 'avr.widget.impression':
                        continue

                    date = datetime.datetime.strptime(info['happened_at'], "%Y-%m-%dT%H:%M:%S+00:00")
                    master_business_id = info['master_business_id']
                    integration_settings_id = info['integration_settings_id']
                    ip_address = info['ip_address']
                    product = 'reviews' if info['product'] == 'home' else info['product']
                    device_type = info['deviceType'] if 'deviceType' in info else 'unknown'
                    referrer_url = info['referrerUrl'] if 'referrerUrl' in info and info['referrerUrl'] != None and \
                                                          info['referrerUrl'] != '' else None
                    tuple = (
                        s3_id,
                        date,
                        master_business_id,
                        integration_settings_id,
                        ip_address,
                        product,
                        device_type,
                        referrer_url,
                    )

                    tuples.append(tuple)

                tuples = set(tuples)

                if len(tuples) > 0:
                    with etl_connection.cursor() as cursor:
                        psycopg2.extras.execute_values(cursor, insert_query, tuples)
                        status = 'success'
                        last_update = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
                        run_time = last_update - start_time
                etl_connection.commit()

    except Exception as e:
        status = str(e)
        sys.exit(1)
    finally:
        etl_connection.close()

    scheduler_connection = postgreshandler.get_analytics_connection()
    postgreshandler.update_script_schedule(scheduler_connection, script, now, status, run_time, last_update)
    scheduler_connection.commit()
    scheduler_connection.close()