"""
timethis.py

Author : David Beazley
         http://www.dabeaz.com
         Copyright (C) 2010

timethis is a utility library for making simple timing benchmarks.  A
single function timethis() is provided.   The function operates as
either a context manager or a decorator.  Here are some examples.

If you want to time a block of code, do this:

with timethis("Counting to a million"):
     n = 0
     while n < 1000000:
         n += 1

The string in quotes is a description that describes the code block
in question.   It will be printed in the output.

If you want to time a function, you can use a decorator:

@timethis
def count_to_a_million():
    n = 0
    while n < 1000000:
        n += 1

count_to_a_million()

All timing output is collected and not printed until a program
exits.  If any code block or function marked with timethis() is
executed more than once, timing measurements are collected 
and used to calculate a mean and standard deviation.
"""
import sys

if sys.version_info < (2,6):
    if sys.version_info < (2,5):
        raise ImportWarning('with statement not available for Python %s' % sys.version)
    from __future__ import with_statement

import atexit
import time
import math
from   contextlib import contextmanager
from   collections import defaultdict

# Dictionary holding timing measurements
_stats = defaultdict(list)

# Exit processing to print performance results
def _printstats():
    if not _stats:
        return
    maxwidth = max(len(str(key)) for key in _stats)
    for key,times in sorted(_stats.items(),key=lambda x: str(x[0])):
        # Compute average and standard deviation
        mean = sum(times)/float(len(times))
        stddev = math.sqrt(sum((x-mean)**2 for x in times)/len(times))
        print("{0:<{maxwidth}s} : {1:0.5f}s : N={2:5d} : stddev={3:0.5f}".format(
                key,mean,len(times),stddev,maxwidth=maxwidth))

atexit.register(_printstats)

# This utility function is used to perform timing benchmarks
def timethis(what):
    @contextmanager
    def benchmark():
        start = time.time()
        yield
        end = time.time()
        _stats[what].append(end-start)
    if hasattr(what,"__call__"):
        def timed(*args,**kwargs):
            with benchmark():
                return what(*args,**kwargs)
        return timed
    else:
        return benchmark()

# Example
if __name__ == '__main__':
    # A single measurement
    with timethis("count to ten million"):
        n = 0
        while n < 10000000:
            n += 1

    # Repeated measurements
    for i in range(10):
        with timethis("count to one million"):
             n = 0
             while n < 1000000:
                 n += 1

    # A function call
    @timethis
    def count_to_a_million():
        n = 0
        while n < 1000000:
            n += 1

    count_to_a_million()
    count_to_a_million()
    count_to_a_million()
