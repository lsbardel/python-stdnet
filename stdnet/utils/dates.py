import time
from time import mktime
from datetime import datetime, timedelta, date


def date2timestamp(dte):
    '''Convert a *dte* into a valid unix timestamp.'''
    seconds = mktime(dte.timetuple())
    if isinstance(dte,datetime):
        return seconds + dte.microsecond / 1000000.0
    else:
        return int(seconds)


def timestamp2date(tstamp):
    "Converts a unix timestamp to a Python datetime object"
    return datetime.fromtimestamp(float(tstamp))

    
def todatetime(dt):
    if isinstance(dt,datetime):
        return dt
    else:
        return datetime(dt.year,dt.month,dt.day)
    
    
def todate(dt):
    if isinstance(dt,datetime):
        return dt.date()
    else:
        return dt
    
    
def default_parse_interval(dt, delta = 0):
    if delta:
        return dt + timedelta(delta)
    else:
        return dt
    
    
def missing_intervals(startdate, enddate, start, end,
                      dateconverter = None,
                      parseinterval = None):
    '''Given a ``startdate`` and an ``enddate`` dates, evaluate the
date intervals from which data is not available. It return a list of
two-dimensional tuples containing start and end date for the interval.
The list could countain 0,1 or 2 tuples.'''
    parseinterval = parseinterval or default_parse_interval
    dateconverter = dateconverter or todate
    startdate = dateconverter(parseinterval(startdate,0))
    enddate   = max(startdate,dateconverter(parseinterval(enddate,0)))

    calc_intervals = []
    # we have some history already
    if start:
        # the startdate is already in the database
        if startdate < start:
            calc_start = startdate
            calc_end = parseinterval(start, -1)
            if calc_end >= calc_start:
                calc_intervals.append((calc_start, calc_end))

        if enddate > end:
            calc_start = parseinterval(end, 1)
            calc_end = enddate
            if calc_end >= calc_start:
                calc_intervals.append((calc_start, calc_end))
    else:
        start = startdate
        end = enddate
        calc_intervals.append((startdate, enddate))

    if calc_intervals:
        # There are calculation intervals, which means the
        # start and aned date have changed
        N = len(calc_intervals)
        start1 = calc_intervals[0][0]
        end1 = calc_intervals[N - 1][1]
        if start:
            start = min(start, start1)
            end = max(end, end1)
        else:
            start = start1
            end = end1

    return calc_intervals  


def dategenerator(start, end, step = 1, desc = False):
    '''Generates dates between *atrt* and *end*.'''
    delta = timedelta(abs(step))
    end = max(start,end)
    if desc:
        dt = end
        while dt>=start:
            yield dt
            dt -= delta
    else:
        dt = start
        while dt<=end:
            yield dt
            dt += delta
            