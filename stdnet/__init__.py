'''A networked standard template library for Python.'''
VERSION = (0, 4, 1)
 
def get_version():
    if len(VERSION) == 3:
        v = '%s.%s.%s' % VERSION
    else:
        v = '%s.%s' % VERSION[:2]
    return v
 
__version__ = get_version()
__license__ = "BSD"
__author__ = "Luca Sbardella"
__contact__ = "luca.sbardella@gmail.com"
__homepage__ = "http://code.google.com/p/python-stdnet/"


from exceptions import *

from backends import *

def add2path():
    import os
    import sys
    path = os.path.split(os.path.split(os.path.abspath(__file__))[0])[0]
    if path not in sys.path:
        sys.path.insert(0,path)
        
        
def runtests(backend = 'redis://127.0.0.1:6379/?db=13'):
    from stdnet.conf import settings
    std = settings.DEFAULT_BACKEND
    settings.DEFAULT_BACKEND = backend
    import unittest
    add2path()
    from stdnet import tests
    loader = unittest.TestLoader()
    suite  = loader.loadTestsFromModule(tests)
    runner = unittest.TextTestRunner()
    runner.run(suite)
    settings.DEFAULT_BACKEND = std

def runbench(backend = 'redis://127.0.0.1:6379/?db=13'):
    from stdnet.conf import settings
    std = settings.DEFAULT_BACKEND
    settings.DEFAULT_BACKEND = backend
    settings.DEFAULT_KEYPREFIX = 'stdbench.'
    from stdnet.utils import bench
    suite = bench.loadBenchFromModules(['stdnet.bench.*'])
    bench.run(suite)