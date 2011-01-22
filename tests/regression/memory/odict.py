from itertools import izip
from datetime import date
import unittest

from stdnet.utils import OrderedSet, OrderedDict, populate


class TestOrderedSet(unittest.TestCase):
    
    def setUp(self):
        self.data = populate(datatype = 'date', size = 100)
    
    def _testSetItem(self):
        s = OrderedSet()
        for v in self.data:
            s.add(v)
        kp = None
        for k in s:
            if kp:
                self.assertTrue(k > kp)
            kp = k
    

class TestOrderedDict(unittest.TestCase):
    
    def setUp(self):
        keys = populate(datatype = 'date', size = 100)
        vals = populate(datatype = 'string', size = 100)
        self.data = izip(keys,vals)
    
    def _testSetItem(self):
        d = OrderedDict()
        for k,v in self.data:
            d[k] = v
        kp = None
        for k in d:
            if kp:
                self.assertTrue(k > kp)
            kp = k
        
    def _testInit(self):
        d = OrderedDict(self.data)
        kp = None
        for k in d:
            if kp:
                self.assertTrue(k > kp)
            kp = k


    