import time
from datetime import datetime

from stdnet.lib.py2py3.py2py3 import *
if ispy3k:
    import pickle
else:
    import cPickle as pickle

#from .encoding import *
from .rwlock import *
from .jsontools import *
from .populate import populate
from .fields import *
    
    
class NoValue(object):
    
    def __repr__(self):
        return '<NoValue>'
    __str__ = __repr__
    

novalue = NoValue()



def date2timestamp(dte):
    return time.mktime(dte.timetuple())


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
        
