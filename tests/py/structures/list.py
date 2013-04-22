from stdnet import odm
from stdnet.utils import test, encoders

from .base import StructMixin


class TestList(StructMixin, test.TestCase):
    structure = odm.List
    name = 'list'
    
    def create_one(self):
        l = odm.List()
        l.push_back(3)
        l.push_back(5.6)
        return l
    
    def test_meta2(self):
        l = yield self.test_meta()
        self.assertTrue(l.session)
        self.assertFalse(l.session.transaction)
        yield l.push_back('save')
        yield l.push_back({'test': 1})
        self.asserGroups(l)
        yield self.async.assertEqual(l.size(), 4)
        result = [3,5.6,'save',"{'test': 1}"]
        yield self.async.assertEqual(l.items(), result)
        
    def testJsonList(self):
        with self.session().begin() as t:
            l = t.add(odm.List(value_pickler=encoders.Json()))
            l.push_back(3)
            l.push_back(5.6)
            l.push_back('save')
            l.push_back({'test': 1})
            l.push_back({'test': 2})
        yield t.on_result
        yield self.async.assertEqual(l.size(), 5)
        result = [3,5.6,'save',{'test': 1},{'test': 2}]
        yield self.async.assertEqual(l.items(), result)
        self.assertEqual(list(l), result)
