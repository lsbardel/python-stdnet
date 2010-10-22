import unittest
from stdnet import orm


class TestCase(unittest.TestCase):
    
    def __init__(self, *args, **kwargs):
        self.orm = orm
        super(TestCase,self).__init__(*args, **kwargs)
        
    def unregister(self):
        pass
    
    def tearDown(self):
        orm.clearall()
        self.unregister()