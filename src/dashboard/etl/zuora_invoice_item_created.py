import sys
sys.path.insert(0,'../..')
import boto3
import src.settings
import s3streaming
import io
import zipfile
import sys
import json
import postgreshandler
import os
import datetime
import psycopg2
import psycopg2.extras
import pytz

zuora_insert_query =    '''
                            INSERT INTO 
                                zuora_invoice_item_created
                            (
                                s3_id,
                                event_id,
                                happened_at,
                                account_account_number,
                                invoice_id,
                                invoice_created_date,
                                product_rate_plan_charge_charge_type,
                                product_rate_plan_charge_uom,
                                invoice_item_id,
                                invoice_item_charge_amount,
                                invoice_item_created_date,
                                invoice_item_uom,
                                product_plan_charge_rate_charge_type,
                                subscription_creator_account_id
                            )
                            VALUES 
                                %s
                        '''

script = os.path.basename(__file__)

s3_completed_files = []
with postgreshandler.get_dashboard_connection() as connection:
    for file in postgreshandler.get_s3_completed_files(connection,script):
        s3_completed_files.append(file)
    s3_completed_files = set(s3_completed_files)

bucket = src.settings.s3_firehose_bucket

client = boto3.client('s3')

resource = boto3.resource('s3')

objects = resource.Bucket(bucket).objects.all()

for object_summary in objects:
    last_modified = object_summary.last_modified
    print(last_modified)
    if last_modified < datetime.datetime(2020,3,31).replace(tzinfo=pytz.utc):
        continue
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
        with postgreshandler.get_dashboard_connection() as connection:
            s3_id = postgreshandler.insert_s3_completed_file(connection, script, file)
            for line in f:
                try:
                    info = json.loads(line)
                    if 'event_name' not in info:
                        continue
                    if info['event_name'] != 'zuora.invoice_item.created':
                        continue

                    event_id = info['event_id'],
                    happened_at = datetime.datetime.strptime(info['happened_at'], "%Y-%m-%dT%H:%M:%S+00:00"),
                    account_account_number = info['Account.AccountNumber'],
                    invoice_id = info['Invoice.Id'],
                    invoice_created_date = datetime.datetime.strptime(info['Invoice.CreatedDate'], "%Y-%m-%dT%H:%M:%S+0000"),
                    product_rate_plan_charge_charge_type = info['ProductRatePlanCharge.ChargeType'],
                    product_rate_plan_charge_uom = info['ProductRatePlanCharge.UOM'] if len(info['ProductRatePlanCharge.UOM']) > 0 else None,
                    invoice_item_id = info['InvoiceItem.Id'],
                    invoice_item_charge_amount = float(info['InvoiceItem.ChargeAmount']),
                    invoice_item_created_date = datetime.datetime.strptime(info['InvoiceItem.CreatedDate'], "%Y-%m-%dT%H:%M:%S+0000"),
                    invoice_item_uom = info['InvoiceItem.UOM'] if len(info['InvoiceItem.UOM']) > 0 else None,
                    product_plan_charge_rate_charge_type = info['ProductRatePlanCharge.ChargeType'],
                    subscription_creator_account_id = info['Subscription.CreatorAccountId']

                    tuple = (
                        s3_id,
                        event_id,
                        happened_at,
                        account_account_number,
                        invoice_id,
                        invoice_created_date,
                        product_rate_plan_charge_charge_type,
                        product_rate_plan_charge_uom,
                        invoice_item_id,
                        invoice_item_charge_amount,
                        invoice_item_created_date,
                        invoice_item_uom,
                        product_plan_charge_rate_charge_type,
                        subscription_creator_account_id,
                    )
                    tuples.append(tuple)
                except json.decoder.JSONDecodeError as e:
                    print(e)
                    continue

            if len(tuples) > 0:
                with connection.cursor() as cursor:
                    psycopg2.extras.execute_values(cursor, zuora_insert_query, tuples)

print('finished')


