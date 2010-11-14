import unittest
from stdnet import orm
from stdnet.utils import bench


class TestCase(unittest.TestCase):
    
    def __init__(self, *args, **kwargs):
        self.orm = orm
        super(TestCase,self).__init__(*args, **kwargs)
        
    def unregister(self):
        pass
    
    def tearDown(self):
        orm.clearall()
        self.unregister()
        
        
class BenchMark(bench.BenchMark):

    def __init__(self, *args, **kwargs):
        self.orm = orm
        super(BenchMark,self).__init__(*args, **kwargs)
        
    def setUp(self):
        self.register()
        orm.clearall()
        
    def register(self):
        pass
        