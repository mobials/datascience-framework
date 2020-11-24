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

schema = 'vendors'
script = os.path.basename(__file__)[:-3]

insert_query =  '''
                        INSERT INTO
                            {0}.{1}
                        (
                            s3_id,
                            id,
                            dealer_id,
                            vin,
                            price,
                            miles,
                            taxonomy_vin,
                            scraped_at,
                            status_date,
                            zip,
                            latitude,
                            longitude,
                            city,
                            state
                        )
                        VALUES 
                            %s
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
        s3_completed_files = []
        for file in postgreshandler.get_s3_scanned_files(etl_connection, script):
            s3_completed_files.append(file)
        s3_completed_files = set(s3_completed_files)

        csv.field_size_limit(sys.maxsize)

        s3 = boto3.resource("s3")

        versions = s3.Bucket(settings.s3_cdc_ca_bucket).object_versions.filter(Prefix=settings.s3_cdc_ca_key)

        for version in versions:
            last_modified = version.last_modified
            print(last_modified)
            file = settings.s3_cdc_ca_bucket + '/' + settings.s3_cdc_ca_key + '/' + version.version_id
            if file in s3_completed_files:
                continue
            obj = version.get()['Body']
            with gzip.GzipFile(fileobj=obj) as gzipfile:
                s3_id = postgreshandler.insert_s3_file(etl_connection, script, file, last_modified)

                tuples = []

                column_headings = None
                count = 0
                for line in gzipfile:
                    count += 1
                    #print(count)
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

                    try:
                        price = float(info['price_fs'])
                    except:
                        continue

                    if price == miles:
                        continue

                    try:
                        dealer_id = int(info['dealer_id_is'])
                    except:
                        continue

                    vin = None if info['vin_ss'] == '' else info['vin_ss']
                    if vin is None:
                        continue

                    taxonomy_vin = None if info['taxonomy_vin_ss'] == '' else info['taxonomy_vin_ss']
                    if taxonomy_vin is None:
                        continue
                    try:
                        if utility.get_vin_pattern(vin) != taxonomy_vin:
                            continue
                    except:
                        continue

                    scraped_at = None if info['scraped_at_dts'] == '' else datetime.datetime.strptime(info['scraped_at_dts'], "%Y-%m-%dT%H:%M:%SZ")
                    if scraped_at is None:
                        continue

                    status_date = None if info['status_date_dts'] == '' else  datetime.datetime.strptime(info['status_date_dts'], "%Y-%m-%dT%H:%M:%SZ")
                    if status_date is None:
                        continue

                    id = None if info['id'] == '' else info['id']

                    _zip = None if info['zip_is'] == '' else info['zip_is'].replace(' ','')

                    latitude = None
                    try:
                        latitude = float(info['latitude_fs'])
                    except:
                        pass

                    longitude = None
                    try:
                        longitude = float(info['longitude_fs'])
                    except:
                        pass

                    city = None if info['city_ss'] == '' else info['city_ss']
                    state = None if info['state_ss'] == '' else info['state_ss']

                    tuple = (
                        s3_id,
                        id,
                        dealer_id,
                        vin,
                        price,
                        miles,
                        taxonomy_vin,
                        scraped_at,
                        status_date,
                        _zip,
                        latitude,
                        longitude,
                        city,
                        state
                    )

                    tuples.append(tuple)



                if len(tuples) > 0:
                    with etl_connection.cursor() as cursor:
                        psycopg2.extras.execute_values(cursor,insert_query,tuples)
                        status = 'success'
                        last_update = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
                        run_time = last_update - start_time
                etl_connection.commit()

    except Exception as e:
        status = str(e)
        print(e)
    finally:
        etl_connection.close()

    #update the scheduler
    scheduler_connection = postgreshandler.get_analytics_connection()
    postgreshandler.update_script_schedule(scheduler_connection,schema,script,now,status,run_time,last_update)
    scheduler_connection.commit()
    scheduler_connection.close()