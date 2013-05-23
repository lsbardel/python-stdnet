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
    
    def test_items(self):
        l = yield self.test_meta()
        self.assertFalse(l.session.transaction)
        yield l.push_back('save')
        yield l.push_back({'test': 1})
        yield self.async.assertEqual(l.size(), 4)
        result = [3,5.6,'save',"{'test': 1}"]
        yield self.async.assertEqual(l.items(), result)
        
    def testJsonList(self):
        models = self.mapper
        l = models.register(self.structure(value_pickler=encoders.Json()))
        self.assertIsInstance(l.value_pickler, encoders.Json)
        self.assertTrue(l.id)
        session = models.session()
        with session.begin() as t:
            t.add(l)
            l.push_back(3)
            l.push_back(5.6)
            l.push_back('save')
            l.push_back({'test': 1})
            l.push_back({'test': 2})
            self.assertEqual(len(l.cache.back), 5)
        yield t.on_result
        yield self.async.assertEqual(l.size(), 5)
        result = [3, 5.6, 'save', {'test': 1}, {'test': 2}]
        yield self.async.assertEqual(l.items(), result)
        self.assertEqual(list(l), result)

    def test_pop_front(self):
        list = yield self.not_empty()
        elem = yield list.pop_front()
        self.assertEqual(elem, 3)
        elem = yield list.pop_front()
        self.assertEqual(elem, 5.6)
        elem = yield list.pop_front()
        self.assertEqual(elem, None)
        
    def test_pop_back(self):
        list = yield self.not_empty()
        elem = yield list.pop_back()
        self.assertEqual(elem, 5.6)
        elem = yield list.pop_back()
        self.assertEqual(elem, 3)
        elem = yield list.pop_back()
        self.assertEqual(elem, None)
        
    def test_block_pop_front(self):
        list = yield self.not_empty()
        elem = yield list.block_pop_front(1)
        self.assertEqual(elem, 3)
        elem = yield list.block_pop_front(1)
        self.assertEqual(elem, 5.6)
        elem = yield list.block_pop_front(1)
        self.assertEqual(elem, None)
        
    def test_block_pop_back(self):
        list = yield self.not_empty()
        elem = yield list.block_pop_back(1)
        self.assertEqual(elem, 5.6)
        elem = yield list.block_pop_back(1)
        self.assertEqual(elem, 3)
        elem = yield list.block_pop_back(1)
        self.assertEqual(elem, None)
        