import datetime
import pytz
import os
import dateutil
from dateutil.relativedelta import relativedelta
import math

def add_days(date,days):
    result = date + datetime.timedelta(days=days)
    result.replace(tzinfo = date.tzinfo)
    return result

def add_hours(date,hours):
    result = date + datetime.timedelta(hours=hours)
    result.replace(tzinfo = date.tzinfo)
    return result

def add_minutes(date,minutes):
    result = date + datetime.timedelta(minutes=minutes)
    result.replace(tzinfo=date.tzinfo)
    return result

def add_months(date,months):
    result = date + relativedelta(months=months)
    return result

def cdc_body_type_to_dataone(body_type,vehicle_type):
    lower_body_type = body_type.lower()
    lower_vehicle_type = vehicle_type.lower()
    result = body_type
    if lower_body_type == 'suv':
        result = 'SUV'
    elif lower_body_type == 'utility':
        result = 'SUV'
    elif lower_body_type == 'crossover':
        result = 'SUV'
    elif lower_body_type == 'sedan':
        result = 'Sedan'
    elif lower_body_type == 'micro car':
        result = 'Sedan'
    elif lower_body_type == 'pickup':
        result = 'Pickup'
    elif lower_body_type == 'truck':
        result = 'Pickup'
    elif lower_body_type == 'chassis':
        result = 'Pickup'
    elif lower_body_type == 'hatchback':
        result = 'Hatchback'
    elif lower_body_type == 'wagon':
        result = 'Wagon'
    elif lower_body_type == 'van':
        result = 'Wagon'
    elif lower_body_type == 'mini-van':
        result = 'Wagon'
    elif lower_body_type == 'minivan':
        result = 'Wagon'
    elif lower_body_type == 'cargo van':
        result = 'Wagon'
    elif lower_body_type == 'mini mpv':
        result = 'Wagon'
    elif lower_body_type == 'convertible':
        result = 'Convertible'
    elif lower_body_type == 'coupe':
        result = 'Coupe';
    elif lower_vehicle_type == 'van':
        result = 'Wagon';
    elif lower_vehicle_type == 'truck':
        result = 'Pickup';
    elif lower_vehicle_type == 'car':
        result = 'Sedan';
    elif lower_vehicle_type == 'suv':
        result = 'SUV';
    return result;

def get_next_run(start_date,last_run,frequency):
    diff = last_run - start_date
    units = int(diff / frequency)
    next_run = start_date + (frequency * (units + 1))
    return next_run

def get_day(date):
    result = date.replace(hour=0, minute=0, second=0, microsecond=0)
    return result

def get_month(date):
    result = date.replace(day=1,hour=0, minute=0, second=0, microsecond=0)
    return result

def get_week(date):
    result = get_day(date - datetime.timedelta(days=date.weekday()))
    return result

def get_quarter(date):
    q = math.ceil(date.month/3.)
    month = 1 if q == 1 else 4 if q == 2 else 7 if q == 3 else 10
    result = get_day(date.replace(month=month,day = 1))
    return result

def get_days_from(start_date,end_date):
    start_date = get_day(start_date)
    end_date = get_day(end_date)
    while start_date < end_date:
        yield  start_date
        start_date = add_days(start_date,1)

def get_weeks_from(start_date,end_date):
    start_date = get_week(start_date)
    end_date = get_week(end_date)
    while start_date < end_date:
        yield  start_date
        start_date = add_days(start_date,7)

def get_months_from(start_date,end_date):
    start_date = get_month(start_date)
    end_date = get_month(end_date)
    while start_date < end_date:
        yield start_date
        start_date = (start_date + datetime.timedelta(days=32)).replace(day=1)

def get_quarters_from(start_date,end_date):
    start_date = get_quarter(start_date)
    end_date = get_quarter(end_date)
    while start_date < end_date:
        yield start_date
        start_date = add_months(start_date,3)

def get_vin_pattern(vin):
    if len(vin) != 17:
        raise Exception('''Invalid VIN {0}'''.format(vin))
    result = vin[0:8] + vin[9:11]
    return result

def extract_domain(url):
    result = None
    return result

