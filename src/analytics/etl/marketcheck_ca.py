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
                            heading,
                            price,
                            miles,
                            stock_no,
                            year,
                            make,
                            model,
                            trim,
                            vehicle_type,
                            body_type,
                            body_subtype,
                            drivetrain,
                            fuel_type,
                            engine,
                            engine_block,
                            engine_size,
                            engine_measure,
                            engine_aspiration,
                            transmission,
                            speeds,
                            doors,
                            cylinders,
                            interior_color,
                            exterior_color,
                            taxonomy_vin,
                            scraped_at,
                            status_date,
                            source,
                            domain_id,
                            more_info,
                            zip,
                            latitude,
                            longitude,
                            address,
                            city,
                            city_state,
                            city_state_zip,
                            county,
                            state,
                            street,
                            area,
                            seller_type,
                            seller_email,
                            seller_phone,
                            seller_name_orig,
                            aws,
                            photo_url,
                            listing_type,
                            seller_comments,
                            options,
                            features,
                            photo_links,
                            car_street,
                            car_address,
                            car_city,
                            car_state,
                            car_zip,
                            is_certified,
                            currency_indicator,
                            miles_indicator,
                            trim_orig,
                            trim_r,
                            msrp,
                            decoder_source
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
            #print(last_modified)
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
                    # if count == 3:
                    #     h = 1
                    # if count == 2:
                    #     continue
                    print(count)
                    text = line.decode()
                    split_text = ['{}'.format(x) for x in list(csv.reader([text], delimiter=',', quotechar='"'))[0]]

                    if not column_headings:
                        column_headings = split_text
                        continue
                    else:
                        info = dict(zip(column_headings, split_text))

                    id = None if info['id'] == '' else info['id']
                    dealer_id = None if info['dealer_id_is'] == '' else info['dealer_id_is']
                    vin = None if info['vin_ss'] == '' else info['vin_ss']
                    heading = None if info['heading_ss'] == '' else info['heading_ss']
                    price = None
                    try:
                        price = float(info['price_fs'])
                    except:
                        pass

                    miles = None
                    try:
                        miles = float(info['miles_fs'])
                    except:
                        pass

                    stock_no = None if info['stock_no_ss'] == '' else info['stock_no_ss']

                    year = None
                    try:
                        year = int(info['year_is'])
                    except:
                        pass

                    make = None if info['make_ss'] == '' else info['make_ss']
                    model = None if info['model_ss'] == '' else info['model_ss']
                    trim = None if info['trim_ss'] == '' else info['trim_ss']
                    vehicle_type = None if info['vehicle_type_ss'] == '' else info['vehicle_type_ss']
                    body_type = None if info['body_type_ss'] == '' else info['body_type_ss']
                    body_subtype = None if info['body_subtype_ss'] == '' else info['body_subtype_ss']
                    drivetrain = None if info['drivetrain_ss'] == '' else info['drivetrain_ss']
                    fuel_type = None if info['fuel_type_ss'] == '' else info['fuel_type_ss']
                    engine = None if info['engine_ss'] == '' else info['engine_ss']
                    engine_block = None if info['engine_block_ss'] == '' else info['engine_block_ss']

                    engine_size = None
                    try:
                        engine_size = float(info['engine_size_ss'])
                    except:
                        pass

                    engine_measure = None if info['engine_measure_ss'] == '' else info['engine_measure_ss']
                    engine_aspiration = None if info['engine_aspiration_ss'] == '' else info['engine_aspiration_ss']
                    transmission = None if info['transmission_ss'] == '' else info['transmission_ss']
                    speeds = None
                    try:
                        speeds = int(info['speeds_is'])
                    except Exception as e:
                        pass

                    doors = None
                    try:
                        doors = int(info['doors_is'])
                    except:
                        pass

                    cylinders = None
                    try:
                        cylinders = int(info['cylinders_is'])
                    except:
                        pass

                    interior_color = None if info['interior_color_ss'] == '' else info['interior_color_ss']
                    exterior_color = None if info['exterior_color_ss'] == '' else info['exterior_color_ss']
                    taxonomy_vin = None if info['taxonomy_vin_ss'] == '' else info['taxonomy_vin_ss']
                    scraped_at = None if info['scraped_at_dts'] == '' else datetime.datetime.strptime(info['scraped_at_dts'], "%Y-%m-%dT%H:%M:%SZ")
                    status_date = None if info['status_date_dts'] == '' else  datetime.datetime.strptime(info['status_date_dts'], "%Y-%m-%dT%H:%M:%SZ")
                    source = None if info['source_ss'] == '' else info['source_ss']

                    domain_id = None
                    try:
                        domain_id = int(info['domain_id_ls'])
                    except:
                        pass

                    more_info = None if info['more_info_ss'] == '' else info['more_info_ss']
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

                    address = None if info['address_ss'] == '' else info['address_ss']
                    city = None if info['city_ss'] == '' else info['city_ss']
                    city_state = None if info['city_state_ss'] == '' else info['city_state_ss']
                    city_state_zip = None if info['city_state_zip_ss'] == '' else info['city_state_zip_ss']
                    county = None if info['county_ss'] == '' else info['county_ss']
                    state = None if info['state_ss'] == '' else info['state_ss']
                    street = None if info['street_ss'] == '' else info['street_ss']
                    area = None if info['area_ss'] == '' else info['area_ss']
                    seller_type = None if info['seller_type_ss'] == '' else info['seller_type_ss']
                    seller_email = None if info['seller_email_ss'] == '' else info['seller_email_ss']
                    seller_phone = None if info['seller_phone_ss'] == '' else info['seller_phone_ss']
                    seller_name_orig = None if info['seller_name_orig_ss'] == '' else info['seller_name_orig_ss']
                    aws = None if info['aws_ss'] == '' else info['aws_ss']
                    photo_url = None if info['photo_url_ss'] == '' else info['photo_url_ss']
                    listing_type = None if info['listing_type_ss'] == '' else info['listing_type_ss']
                    seller_comments = None if info['seller_comments_texts'] == '' else info['seller_comments_texts']
                    options = None if info['options_texts'] == '' else info['options_texts']
                    features = None if info['features_texts'] == '' else info['features_texts']
                    photo_links = None if info['photo_links_ss'] == '' else info['photo_links_ss']
                    car_street = None if info['car_street_ss'] == '' else info['car_street_ss']
                    car_address = None if info['car_address_ss'] == '' else info['car_address_ss']
                    car_city = None if info['car_city_ss'] == '' else info['car_city_ss']
                    car_state = None if info['car_state_ss'] == '' else info['car_state_ss']
                    car_zip = None if info['car_zip_is'] == '' else info['car_zip_is']

                    is_certified = None
                    try:
                        is_certified = int(info['is_certified_is'])
                    except:
                        pass

                    currency_indicator = None if info['currency_indicator_ss'] == '' else info['currency_indicator_ss']
                    miles_indicator = None if info['miles_indicator_ss'] == '' else info['miles_indicator_ss']
                    trim_orig = None if info['trim_orig_ss'] == '' else info['trim_orig_ss']
                    trim_r = None if info['trim_r_ss'] == '' else info['trim_r_ss']
                    msrp = None

                    try:
                        msrp_fs = float(info['trim_r_ss'])
                    except:
                        pass
                    decoder_source = None if info['decoder_source'] == '' else info['decoder_source']

                    tuple = (
                        s3_id,
                        id,
                        dealer_id,
                        vin,
                        heading,
                        price,
                        miles,
                        stock_no,
                        year,
                        make,
                        model,
                        trim,
                        vehicle_type,
                        body_type,
                        body_subtype,
                        drivetrain,
                        fuel_type,
                        engine,
                        engine_block,
                        engine_size,
                        engine_measure,
                        engine_aspiration,
                        transmission,
                        speeds,
                        doors,
                        cylinders,
                        interior_color,
                        exterior_color,
                        taxonomy_vin,
                        scraped_at,
                        status_date,
                        source,
                        domain_id,
                        more_info,
                        _zip,
                        latitude,
                        longitude,
                        address,
                        city,
                        city_state,
                        city_state_zip,
                        county,
                        state,
                        street,
                        area,
                        seller_type,
                        seller_email,
                        seller_phone,
                        seller_name_orig,
                        aws,
                        photo_url,
                        listing_type,
                        seller_comments,
                        options,
                        features,
                        photo_links,
                        car_street,
                        car_address,
                        car_city,
                        car_state,
                        car_zip,
                        is_certified,
                        currency_indicator,
                        miles_indicator,
                        trim_orig,
                        trim_r,
                        msrp,
                        decoder_source,
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
   # postgreshandler.update_script_schedule(scheduler_connection, script, now, status, run_time, last_update)
    scheduler_connection.commit()
    scheduler_connection.close()