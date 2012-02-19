import time
from time import mktime
from datetime import datetime, timedelta, date
from collections import namedtuple


class Interval(namedtuple('IntervalBase','start end')):
    
    def __init__(self, start, end):
        if start > end:
            raise ValueError('Bad interval.')
    
    def __reduce__(self):
        return tuple,(tuple(self),)
    
    def __contains__(self, value):
        return value >= self.start or value <= self.end
    
    def __lt__(self, other):
        return self.end < other.start
    
    def __gt__(self, other):
        return self.start > other.end
    
    def __eq__(self, other):
        return self.start == other.start and self.end == other.end
    
    def union(self, other):
        return Interval(min(self.start,other.start),
                        max(self.end,other.end))
    
    
class Intervals(list):

    def __init__(self, data = None):
        super(Intervals,self).__init__()
        if data:
            self.extend(data)
            
    def __reduce__(self):
        return list,(list(self),)
    
    def start(self):
        if self:
            return self[0].start
            
    def end(self):
        if self:
            return self[-1].end
    
    def extend(self, data):
        for d in data:
            self.append(d)
            
    def append(self, interval):
        if not isinstance(interval,Interval):
            interval = Interval(*interval)
        for idx,intv in enumerate(self):
            if interval < intv:
                self.insert(idx,interval)
                return
            elif interval > intv:
                continue
            else:
                self[idx] = interval.union(intv)
                return self.check()
        super(Intervals,self).append(interval)
        
    def check(self):
        merged = True
        while merged and len(self) > 1:
            merged = False
            for idx,interval in enumerate(self[:-1]):
                other = self[idx+1]
                if interval < other:
                    continue
                elif interval > other:
                    raise ValueError()
                else:
                    self[idx] = interval.union(other)
                    self.pop(idx+1)
                    merged = True
                    break
                
                
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
                      parseinterval = None,
                      intervals = None):
    '''Given a ``startdate`` and an ``enddate`` dates, evaluate the
date intervals from which data is not available. It return a list of
two-dimensional tuples containing start and end date for the interval.
The list could countain 0,1 or 2 tuples.'''
    parseinterval = parseinterval or default_parse_interval
    dateconverter = dateconverter or todate
    startdate = dateconverter(parseinterval(startdate,0))
    enddate   = max(startdate,dateconverter(parseinterval(enddate,0)))
    
    if intervals is not None and not isinstance(intervals,Intervals):
        intervals = Intervals(intervals)
        
    calc_intervals = Intervals()
    # we have some history already
    if start:
        # the startdate not available
        if startdate < start:
            calc_start = startdate
            calc_end = parseinterval(start, -1)
            if calc_end >= calc_start:
                calc_intervals.append(Interval(calc_start, calc_end))

        if enddate > end:
            calc_start = parseinterval(end, 1)
            calc_end = enddate
            if calc_end >= calc_start:
                calc_intervals.append(Interval(calc_start, calc_end))
    else:
        start = startdate
        end = enddate
        calc_intervals.append(Interval(startdate, enddate))

    if calc_intervals:
        if intervals:
            calc_intervals.extend(intervals)
    elif intervals:
        calc_intervals = intervals

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
            