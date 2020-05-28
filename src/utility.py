import datetime
import pytz
import os

def add_days(date,days):
    result = date + datetime.timedelta(days=days)
    result.replace(tzinfo = date.tzinfo)
    return result

def get_day(date):
    result = datetime.datetime(date.year,date.month,date.day).replace(tzinfo=date.tzinfo)
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

def round_date_to_next_interval(start_date,last_run,interval):
    f = last_run - start_date
    y = f/interval

    return result

