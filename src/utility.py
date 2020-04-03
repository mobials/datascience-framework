import datetime
import pytz

def add_days(date,days):
    result = date + datetime.timedelta(days=days)
    return result