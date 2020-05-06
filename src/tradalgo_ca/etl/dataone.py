import sys
sys.path.insert(0,'../..')
import psycopg2
import postgreshandler
import os
import boto3
import settings
import datetime
import pytz
import gzip
import csv
import re

script = os.path.basename(__file__)[:-3]

insert_query =  '''
                    INSERT INTO 
                        dataone
                    (
                        s3_id,
                        vin_pattern,
                        vehicle_id,
                        market, 
                        year, 
                        make,
                        model,
                        trim,
                        style,
                        body_type,
                        msrp
                    )
                    VALUES 
                        %s
                    ON CONFLICT ON CONSTRAINT dataone_vin_pat_veh_id_idx
                    DO UPDATE 
                    SET 
                        s3_id = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                        then EXCLUDED.s3_id else dataone.s3_id end,
                        market = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                        then EXCLUDED.market else dataone.market end,
                        year = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                        then EXCLUDED.year else dataone.year end,
                        make = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                            then EXCLUDED.make else dataone.make end,
                        model = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                        then EXCLUDED.model else dataone.model end,
                        trim = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                        then EXCLUDED.trim else dataone.trim end,
                        style = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                        then EXCLUDED.style else dataone.style end,
                        body_type = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                        then EXCLUDED.body_type else dataone.body_type end,
                        msrp = case when (select last_modified from s3 where id = dataone.s3_id) > (select last_modified from s3 where id = excluded.s3_id)
                                        then EXCLUDED.msrp else dataone.msrp end
                '''

def transform_info(info):
    if 'MAZDA' in info['MODEL']:
        info['MODEL'] = info['MODEL'].title()

    if info['MAKE'] == 'Toyota' and info['MODEL'] == 'RAV4' and info['TRIM'] == 'Adventure':
        info['TRIM'] = 'Trail'
        info['STYLE'] = info['STYLE'].replace('Adventure', 'Trail')

    info['STYLE'] = info['STYLE'].replace('4X4', '4x4')

    if info['BODY_TYPE'] == 'Chassis':
        info['BODY_TYPE'] = 'Pickup'

    if info['BODY_TYPE'] == 'Mini-Van':
        info['BODY_TYPE'] = 'Wagon'

    if info['MAKE'] == 'Hyundai' and info['MODEL'] == 'Veracruz':
        info['BODY_TYPE'] = 'SUV'

    info['STYLE'] = re.sub(' [0-9]*.[0-9]* ft. ', ' ')

    return info

last_modified = None
with postgreshandler.get_tradalgo_canada_connection() as connection:
    last_modified = postgreshandler.get_s3_scanned_max_last_modified_date(connection,script)
if last_modified is None:
    last_modified = datetime.datetime(2020,1,1).replace(tzinfo=pytz.utc)

bucket = settings.s3_dataone_bucket
key = settings.s3_dataone_key

resource = boto3.resource('s3')

object = resource.Object(bucket, key)

file = bucket + '/' + key + '/' + object.version_id

if object.last_modified > last_modified:
    body = object.get()['Body']
    with gzip.GzipFile(fileobj=body) as gzipfile:

        s3_id = postgreshandler.insert_s3_file(connection, script, file, last_modified)
        tuples = []

        column_headings = None
        count = 0
        for line in gzipfile:
            count += 1
            text = line.decode()
            split_text = ['{}'.format(x) for x in list(csv.reader([text], delimiter='\t'))[0]]

            if not column_headings:
                column_headings = split_text
                continue
            else:
                info = dict(zip(column_headings, split_text))

                if '(ends ' in info['STYLE'] :
                    continue

                if int(info['YEAR']) < datetime.datetime.utcnow().year - 20:
                    continue

                info = transform_info(info)

                vin_pattern = info['VIN_PATTERN']
                vehicle_id = int(info['VEHICLE_ID'])
                market = info['MARKET']
                year = int(info['YEAR'])
                make = info['MAKE']
                model = info['MODEL']
                trim = info['TRIM']
                style = info['STYLE']
                body_type = info['BODY_TYPE']
                msrp = float(info['MSRP'])  if info['MSRP'] != '' else None

                tuple = (
                    s3_id,
                    vin_pattern,
                    vehicle_id,
                    market,
                    year,
                    make,
                    trim,
                    style,
                    body_type,
                    msrp
                )

                tuples.append(tuple)


