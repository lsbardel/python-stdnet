'''\
A timeseries application where each field is stored in a redis string.
This datastructure is composed by several redis structure:

* A Timeseries for holding times in an ordered fashion.
* A redis *set* for holding of *fields* names.
* A redis string for each *field* in the timeseries.

The data in a given field string is stored in a 9-byte sequence with the first
byte indicating the type of data.
'''
from . import redis
from .models import *