import sys
sys.path.insert(0,'../..')
import requests
import settings
import time
import csv
import postgreshandler
import datetime
import utility
import pytz
import psycopg2
import psycopg2.extras

def execute_queries(query_payload,headers):
    query_url = settings.zuora_base_api_url + '/v1/batch-query/'
    request = requests.post(query_url, json=query_payload, headers=headers)
    payload = request.json()
    if payload['status'] == 'error':
        raise Exception('Aqua API Error.  Error code:',payload['errorCode'],'Message:',payload['message'])
    job_id = payload["id"]
    job_url = query_url + "/jobs/{}".format(job_id)
    file_ids = None
    while True:
        request = requests.get(job_url, headers=headers)
        payload = request.json()
        status = payload['status']
        if status == 'completed':
            result = payload['batches']
            break
        else:
            time.sleep(1)
    return result

def create_headers(bearer_token):
    result = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {bearer_token}",
    }
    return result

def get_file_data(file_id,headers):
    file_url = settings.zuora_base_url + "/apps/api/file/{}".format(file_id)
    request = requests.get(file_url, headers=headers)
    result = request.content.decode("utf-8")
    return result

def create_bearer_token(client_id, client_secret):
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }
    request = requests.post(settings.zuora_base_api_url + "/oauth/token", data=payload)
    request.raise_for_status()
    result = request.json()["access_token"]
    return result

def create_queries(connection,tables):
    queries = []
    for table in tables:
        query = create_query(connection,table)
        queries.append(query)
    result = {
        "format": "csv",
        "version": "1.0",
        "name": "replication-client",
        "useQueryLabels": "true",
        "encrypted": "none",
        "queries": queries
    }
    return result

def create_query(connection,table):
    min_updated_date = get_max_updated_date(connection,table).replace(tzinfo = pytz.utc)
    max_updated_date = utility.add_hours(datetime.datetime.utcnow(),-2).replace(tzinfo = pytz.utc)

    min_updated_date_string = min_updated_date.strftime("%Y-%m-%dT%H:%M:%S")
    max_updated_date_string = max_updated_date.strftime("%Y-%m-%dT%H:%M:%S")

    query = '''
                SELECT 
                    * 
                FROM 
                    {0} 
                WHERE 
                    updateddate >= '{1}' 
                AND
                    updateddate < '{2}'
            '''.format(table,min_updated_date_string,max_updated_date_string)

    result = {
                "name": table,
                "query": query,
                "type": "zoqlexport",
                "apiVersion": "96.0",
                "deleted": {
                    "column": "is_deleted",
                    "format": "Boolean",
                    "forceExport": "true"
                }
            }
    return result

def get_max_updated_date(connection,table):
    result = datetime.datetime(2000,1,1)
    query = '''
                SELECT 
                    max(updateddate)
                FROM
                    zuora_{0}
            '''.format(table)

    with connection.cursor() as cursor:
        cursor.execute(query)
        record = cursor.fetchone()
        if record is not None and record[0] is not None:
            result = record[0]
    return result