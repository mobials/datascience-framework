import sys
sys.path.insert(0,'../..')
sys.path.append('/var/www/datascience-framework/src/')
import boto3
import settings
import io
import zipfile
import json
import postgreshandler
import os
import datetime
import psycopg2
import psycopg2.extras
import pytz
import time
import utility

schema = 'aggregations'
script = os.path.basename(__file__)[:-3]
days = 14
timelapse_seconds = 86400
query =     '''
                CREATE TEMP TABLE avr_client_new_listings_ca_new_temp ON COMMIT DROP AS 
                SELECT 
                    a.master_business_id, 
                    a.vin,
                    a.status_date
                FROM 
                    aggregations.avr_client_inventory_ca_new a
                WHERE 
                    NOT EXISTS 
                    (
                        SELECT 
                            1 
                        FROM 
                            vendors.marketcheck_ca_new 
                        WHERE 
                            dealer_id = a.dealer_id 
                        AND
                            vin = a.vin 
                        AND 
                            status_date < a.status_date - INTERVAL '{2} days'
                    );
                TRUNCATE TABLE {0}.{1};
                INSERT INTO 
                    {0}.{1}
                SELECT 
                    * 
                FROM 
                    avr_client_new_listings_ca_new_temp;
        '''.format(schema,script,days)

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

    postgres_etl_connection = postgreshandler.get_analytics_connection()
    try:
        with postgres_etl_connection.cursor() as cursor:

            max_marketcheck_status_date = postgreshandler.get_max_value(postgres_etl_connection, 'vendors',
                                                                        'marketcheck_ca_new','status_date')

            max_avr_client_inventory_status_date = postgreshandler.get_max_value(postgres_etl_connection, 'aggregations',
                                                                        'avr_client_inventory_ca_new', 'status_date')

            max_avr_client_new_listings_status_date = postgreshandler.get_max_value(postgres_etl_connection, schema, script,
                                                                      'status_date')
            if max_avr_client_new_listings_status_date is None:
                max_avr_client_new_listings_status_date = datetime.datetime(2000, 1, 1).replace(tzinfo=pytz.utc)

            #only proceed if we have client inventory data
            if max_marketcheck_status_date is None:
                raise Exception('Waiting for MarketCheck inventory')

            if max_avr_client_inventory_status_date is None:
                raise Exception('Waiting for Inventory data')

            diff1 = max_marketcheck_status_date - max_avr_client_new_listings_status_date
            diff2 = max_avr_client_inventory_status_date - max_avr_client_new_listings_status_date
            if diff1.total_seconds() >= timelapse_seconds and diff2.total_seconds() >= timelapse_seconds:
                cursor.execute(query)
                postgres_etl_connection.commit()
                status = 'success'
                last_update = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
                run_time = last_update - start_time
    except Exception as e:
        status = str(e)
    finally:
        postgres_etl_connection.close()

    scheduler_connection = postgreshandler.get_analytics_connection()
    postgreshandler.update_script_schedule(scheduler_connection, schema, script, now, status, run_time, last_update)
    scheduler_connection.commit()
    scheduler_connection.close()