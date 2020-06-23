import sys
sys.path.insert(0,'../..')
sys.path.append('/var/www/datascience-framework/src/')
import boto3
import csv
import datetime
import psycopg2
import psycopg2.extras
import postgreshandler
import utility
import pytz
import settings
import os
import gzip
import time

script = os.path.basename(__file__)[:-3]

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
                        ON CONFLICT ON CONSTRAINT cdc_vin_pk
                        DO UPDATE 
                        SET 
                            status_date = case when cdc.status_date < EXCLUDED.status_date then EXCLUDED.status_date else cdc.status_date end,
                            s3_id = case when cdc.status_date < EXCLUDED.status_date then EXCLUDED.s3_id else cdc.s3_id end,
                            miles = case when cdc.status_date < EXCLUDED.status_date then EXCLUDED.miles else cdc.miles end,
                            price = case when cdc.status_date < EXCLUDED.status_date then EXCLUDED.price else cdc.price end,
                            body_type = case when cdc.status_date < EXCLUDED.status_date then EXCLUDED.body_type else cdc.body_type end,
                            trim_orig = case when cdc.status_date < EXCLUDED.status_date then EXCLUDED.trim_orig else cdc.trim_orig end,
                            taxonomy_vin = case when cdc.status_date < EXCLUDED.status_date then EXCLUDED.taxonomy_vin else cdc.taxonomy_vin end
                    '''

while True:
    schedule_info = None
    scheduler_connection = postgreshandler.get_tradalgo_canada_connection()
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
    etl_connection = postgreshandler.get_tradalgo_canada_connection()
    try:
        s3_completed_files = []
        for file in postgreshandler.get_s3_scanned_files(etl_connection, script):
            s3_completed_files.append(file)
        s3_completed_files = set(s3_completed_files)

        csv.field_size_limit(sys.maxsize)

        s3 = boto3.resource("s3")

        versions = s3.Bucket(settings.s3_cdc_ca_bucket).object_versions.filter(Prefix=settings.s3_cdc_ca_key)

        for version in versions:
            last_modified = version.last_modified
            #print(last_modified)
            file = settings.s3_cdc_ca_bucket + '/' + settings.s3_cdc_ca_key + '/' + version.version_id
            if file in s3_completed_files:
                continue
            obj = version.get()['Body']
            with gzip.GzipFile(fileobj=obj) as gzipfile:
                s3_id = postgreshandler.insert_s3_file(etl_connection, script, file, last_modified)

                tuples = {}

                column_headings = None
                for line in gzipfile:
                    text = line.decode()
                    split_text = ['{}'.format(x) for x in list(csv.reader([text], delimiter=',', quotechar='"'))[0]]

                    if not column_headings:
                        column_headings = split_text
                        continue
                    else:
                        info = dict(zip(column_headings, split_text))

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

                    if price == miles:
                        continue

                    status_date = datetime.datetime.strptime(info['status_date_dts'], "%Y-%m-%dT%H:%M:%SZ")

                    vin = info['vin_ss']
                    taxonomy_vin = info['taxonomy_vin_ss']
                    original_body_type = info['body_type_ss'].strip()
                    vehicle_type = info['vehicle_type_ss'].strip()
                    body_type = utility.cdc_body_type_to_dataone(original_body_type,
                                                                 vehicle_type) if original_body_type is not '' else None
                    trim_orig = info['trim_orig_ss'] if 'trim_orig_ss' in info and info[
                        'trim_orig_ss'] is not '' else None

                    tuple = (
                        vin,
                    )

                    if tuple in tuples:
                        existing_values = tuples[tuple]
                        existing_status_date = existing_values[2]
                        if existing_status_date < status_date:
                            tuples[tuple] = (miles, price, status_date, taxonomy_vin, body_type, trim_orig, s3_id)
                    else:
                        tuples[tuple] = (miles, price, status_date, taxonomy_vin, body_type, trim_orig, s3_id)

                tuples = [key + value for key, value in tuples.items()]

                if len(tuples) > 0:
                    with etl_connection.cursor() as cursor:
                        psycopg2.extras.execute_values(cursor, cdc_insert_query, tuples)
                        status = 'success'
                        last_update = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
                        run_time = last_update - start_time
                etl_connection.commit()

    except Exception as e:
        status = str(e)
    finally:
        etl_connection.close()

    #update the scheduler
    scheduler_connection = postgreshandler.get_tradalgo_canada_connection()
    postgreshandler.update_script_schedule(scheduler_connection, script, now, status, run_time, last_update)
    scheduler_connection.commit()
    scheduler_connection.close()