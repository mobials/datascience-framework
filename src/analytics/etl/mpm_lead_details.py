import sys
sys.path.insert(0,'../..')
sys.path.append('/var/www/datascience-framework/src/')
import boto3
import settings
import io
import zipfile
import json
import postgreshandler
import mysqlhandler
import os
import datetime
import psycopg2
import psycopg2.extras
import pytz
import time
import utility

schema = 'autoverify'
script = os.path.basename(__file__)[:-3]

insert_query =  '''
                    TRUNCATE TABLE {0}.{1};
                    INSERT INTO {0}.{1}
                    SELECT
                        master_business_id,
                        id,
                        status,
                        created_at,
                        updated_at,
                        customer_first_name,
                        customer_last_name,
                        customer_email,
                        customer_phone,
                        customer_language,
                        customer_mobile_phone,
                        customer_payload,
                        customer_address_line_1,
                        customer_address_line_2,
                        customer_city,
                        customer_postal_code,
                        customer_country,
                        customer_province,
                        trade_high_value,
                        trade_low_value,
                        trade_deductions,
                        trade_customer_referral_url,
                        trade_vehicle_year,
                        trade_vehicle_make,
                        trade_vehicle_model,
                        trade_vehicle_style,
                        trade_vehicle_trim,
                        trade_vehicle_mileage,
                        trade_vehicle_vin,
                        insurance_quote_id,
                        insurance_referrer_url,
                        insurance_quote_payload,
                        insurance_purchase_price,
                        insurance_vehicle_year,
                        insurance_vehicle_make,
                        insurance_vehicle_model,
                        insurance_vehicle_trim,
                        insurance_vehicle_style,
                        insurance_vehicle_mileage,
                        insurance_vehicle_vin,
                        customer_birthdate,
                        customer_tracking_id,
                        credit_rating,
                        credit_dealertrack_sent_at,
                        source_url,
                        credit_vehicle_price,
                        credit_trade_in_value,
                        credit_down_payment,
                        credit_sales_tax,
                        credit_interest_rate,
                        credit_loan_amount,
                        language_code,
                        integration_settings_id,
                        credit_creditor_name,
                        credit_payload,
                        trade_low_list,
                        trade_high_list,
                        trade_list_count,
                        trade_aged_discount,
                        trade_reconditioning,
                        trade_advertising,
                        trade_overhead,
                        trade_dealer_profit,
                        trade_in_source,
                        route_list,
                        experiment,
                        credit_payment_frequency,
                        credit_trade_in_owing,
                        credit_payment_amount,
                        credit_term_in_months,
                        credit_regional_rating,
                        ecom_reserved,
                        testdrive_requested_date,
                        testdrive_time_of_day_preference,
                        testdrive_language,
                        testdrive_gender_preference,
                        testdrive_technology_interests,
                        testdrive_route_name,
                        testdrive_beverage,
                        testdrive_comments,
                        oem_of_interest,
                        billing_key,
                        spincar_lead_report_url,
                        is_insurance,
                        is_trade,
                        is_credit_partial,
                        is_credit_verified,
                        is_credit_finance,
                        is_ecom,
                        is_spotlight,
                        device,
                        is_credit_regional,
                        is_test_drive,
                        lead_content,
                        sub_type,
                        is_accident_check
                    FROM 
                        autoverify.v_mpm_lead_details;

                '''

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
    postgreshandler.update_script_schedule(scheduler_connection,schema,script,now,status,run_time,last_update)
    scheduler_connection.commit()
    scheduler_connection.close()