import time
from datetime import datetime

from encoding import *
from rwlock import *
from importlib import *
from anyjson import *
from odict import *
from populate import populate
from fields import *


def date2timestamp(dte):
    return int(1000*time.mktime(dte.timetuple()))


def timestamp2date(tstamp):
    "Converts a unix timestamp to a Python datetime object"
    return datetime.fromtimestamp(0.001*int(tstamp))
