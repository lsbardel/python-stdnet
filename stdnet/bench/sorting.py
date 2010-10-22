import base
from datetime import date
from timeit import default_timer as timer
from stdnet.utils import populate, date2timestamp

print("Getting data")
v = populate('date',
             100000,
             start = date(1970,1,1),
             converter = date2timestamp)

def timeit(f):
    t1 = timer()
    f()
    dt = timer() - t1
    print("%s executed in %s seconds" % (f.__name__,dt))

def sort1():
    sorted(v)
def sortinline():
    v.sort()
    

print("Start")
timeit(sort1)
timeit(sortinline)