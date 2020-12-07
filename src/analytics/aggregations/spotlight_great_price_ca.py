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
import pandas
import numpy

schema = 'public'
script = os.path.basename(__file__)[:-3]