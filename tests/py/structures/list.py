from stdnet import odm
from stdnet.utils import test, encoders

from .base import StructMixin


class TestList(StructMixin, test.CleanTestCase):
    structure = odm.List
    name = 'list'
    
    def create_one(self):
        l = odm.List()
        l.push_back(3)
        l.push_back(5.6)
        return l
    
    def test_meta2(self):
        l = yield self.test_meta()
        l.push_back('save')
        l.push_back({'test': 1})
        l.session.commit()
        self.asserGroups(l)
        self.assertEqual(l.size(),4)
        self.assertEqual(list(l),[3,5.6,'save',"{'test': 1}"])
        
    def testJsonList(self):
        with self.session().begin() as t:
            l = t.add(odm.List(value_pickler=encoders.Json()))
            l.push_back(3)
            l.push_back(5.6)
            l.push_back('save')
            l.push_back({'test': 1})
            l.push_back({'test': 2})
        self.assertEqual(l.size(),5)
        self.assertEqual(list(l),[3,5.6,'save',{'test': 1},{'test': 2}])
    
