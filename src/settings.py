import pathlib
import sys
import dotenv
import os
import psycopg2

dotenv.load_dotenv()

tradalgo_canada_username = os.getenv('TRADALGO_CANADA_USERNAME')
tradalgo_canada_password = os.getenv('TRADALGO_CANADA_PASSWORD')
tradalgo_canada_database = os.getenv('TRADALGO_CANADA_DATABASE')
tradalgo_canada_port = os.getenv('TRADALGO_CANADA_PORT')
tradalgo_canada_host = os.getenv('TRADALGO_CANADA_HOST')

tradalgo_staging_user = os.getenv('TRADALGO_STAGING_USER')
tradalgo_staging_pass = os.getenv('TRADALGO_STAGING_PASS')
tradalgo_staging_db = os.getenv('TRADALGO_STAGING_DB')
tradalgo_staging_port = os.getenv('TRADALGO_STAGING_PORT')
tradalgo_staging_host = os.getenv('TRADALGO_STAGING_HOST')

datascience_user = os.getenv('DATASCIENCE_USER')
datascience_pass = os.getenv('DATASCIENCE_PASS')
datascience_db = os.getenv('DATASCIENCE_DB')
datascience_port = os.getenv('DATASCIENCE_PORT')
datascience_host = os.getenv('DATASCIENCE_HOST')

metrics_staging_username = os.getenv('METRICS_STAGING_USERNAME')
metrics_staging_password = os.getenv('METRICS_STAGING_PASSWORD')
metrics_staging_database = os.getenv('METRICS_STAGING_DATABASE')
metrics_staging_port = os.getenv('METRICS_STAGING_PORT')
metrics_staging_host = os.getenv('METRICS_STAGING_HOST')

dashboard_username = os.getenv('DASHBOARD_USERNAME')
dashboard_password = os.getenv('DASHBOARD_PASSWORD')
dashboard_database = os.getenv('DASHBOARD_DATABASE')
dashboard_port = os.getenv('DASHBOARD_PORT')
dashboard_host = os.getenv('DASHBOARD_HOST')

analytics_username = os.getenv('ANALYTICS_USERNAME')
analytics_password = os.getenv('ANALYTICS_PASSWORD')
analytics_database = os.getenv('ANALYTICS_DATABASE')
analytics_port = os.getenv('ANALYTICS_PORT')
analytics_host = os.getenv('ANALYTICS_HOST')

#cdc_s3_processing_csn
s3_cdc_ca_bucket=os.getenv('S3_CDC_CA_BUCKET')
s3_cdc_ca_key=os.getenv('S3_CDC_CA_KEY')

#cdc us
s3_cdc_us_bucket=os.getenv('S3_CDC_US_BUCKET')
s3_cdc_us_key=os.getenv('S3_CDC_US_KEY')

#firehose production
s3_firehose_bucket_production = os.getenv('S3_FIREHOSE_BUCKET_PRODUCTION')

#firehose
s3_firehose_bucket = os.getenv('S3_FIREHOSE_BUCKET')

#dataone
s3_dataone_bucket = os.getenv('S3_DATAONE_BUCKET')
s3_dataone_key = os.getenv('S3_DATAONE_KEY')

#authenticom
s3_authenticom = os.getenv('S3_AUTHENTICOM_PRODUCTION')

s3_sda_audit_log_bucket = os.getenv('S3_SDA_AUDIT_LOG_PRODUCTION')

zuora_base_api_url =  os.getenv('ZUORA_BASE_API_URL')
zuora_base_url =  os.getenv('ZUORA_BASE_URL')
zuora_client_id = os.getenv('ZUORA_CLIENT_ID')
zuora_client_secret = os.getenv('ZUORA_CLIENT_SECRET')
