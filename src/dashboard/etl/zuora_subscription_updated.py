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
                                account_status,
                                product_id,
                                product_name,
                                product_sku,
                                product_rate_plan_id,
                                product_rate_plan_name,
                                rate_plan_id,
                                rate_plan_name,
                                subscription_creator_account_id,
                                subscription_creator_invoice_owner_id,
                                subscription_invoice_owner_id,
                                subscription_is_invoice_separate,
                                subscription_original_id,
                                subscription_previous_subscription_id,
                                subscription_id,
                                subscription_status
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
                    if 'event_name' not in info or info['event_name'] != 'zuora.subscription.updated':
                        continue
                    event_id = info['event_id'],
                    happened_at = datetime.datetime.strptime(info['happened_at'], "%Y-%m-%dT%H:%M:%S+00:00"),
                    account_account_number = info['Account.AccountNumber'],
                    account_status = info['Account.Status'],
                    product_id = info['Product.Id'],
                    product_name = info['Product.Name'],
                    product_sku = info['Product.SKU'],
                    product_rate_plan_id = info['ProductRatePlan.Id'],
                    product_rate_plan_name = info['ProductRatePlan.Name'],
                    rate_plan_id = info['RatePlan.Id'],
                    rate_plan_name = info['RatePlan.Name'],
                    subscription_creator_account_id = info['Subscription.CreatorAccountId'],
                    subscription_creator_invoice_owner_id = info['Subscription.CreatorInvoiceOwnerId'],
                    subscription_invoice_owner_id = info['Subscription.InvoiceOwnerId'],
                    subscription_is_invoice_separate = info['Subscription.IsInvoiceSeparate'],
                    subscription_original_id = info['Subscription.OriginalId'],
                    subscription_previous_subscription_id = info['Subscription.PreviousSubscriptionId'],
                    subscription_id = info['Subscription.Id']
                    subscription_status = info['Subscription.Status']

                    tuple = (
                        s3_id,
                        event_id,
                        happened_at,
                        account_account_number,
                        account_status,
                        product_id,
                        product_name,
                        product_sku,
                        product_rate_plan_id,
                        product_rate_plan_name,
                        rate_plan_id,
                        rate_plan_name,
                        subscription_creator_account_id,
                        subscription_creator_invoice_owner_id,
                        subscription_invoice_owner_id,
                        subscription_is_invoice_separate,
                        subscription_original_id,
                        subscription_previous_subscription_id,
                        subscription_id,
                        subscription_status,
                    )
                    tuples.append(tuple)
                except json.decoder.JSONDecodeError as e:
                    print(e)
                    continue

            if len(tuples) > 0:
                with connection.cursor() as cursor:
                    psycopg2.extras.execute_values(cursor, zuora_insert_query, tuples)

print('finished')


