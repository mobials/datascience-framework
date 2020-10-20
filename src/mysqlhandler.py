import sys
sys.path.append('/var/www/datascience-framework/src/')
import mysql.connector
import settings

def get_autoverify_connection():
    result =  mysql.connector.connect(
        host=settings.autoverify_host,
        user=settings.autoverify_username,
        password=settings.autoverify_password,
        port=settings.autoverify_port,
        database=settings.autoverify_database
    )
    return result