from pulsar import multi_async

from stdnet import odm
from stdnet.utils import test

from .base import StructMixin


class TestHash(StructMixin, test.CleanTestCase):
    structure = odm.HashTable
    name = 'hashtable'
    
    def create_one(self):
        h = odm.HashTable()
        h['bla'] = 'foo'
        h['pluto'] = 3
        return h
        
    def testNoTransaction(self):
        session = self.session()
        d = odm.HashTable()
        d['bla'] = 5676
        yield session.add(d)
        yield self.async.assertEqual(d.size(), 1)
        yield self.async.assertEqual(d['bla'], 5676)
        
    def testEmpty(self):
        with self.session().begin() as t:
            d = t.add(odm.HashTable())
        yield t.on_result
        result = yield d.get('bla', 3)
        self.assertEqual(result, 3)
        
    def testPop(self):
        with self.session().begin() as t:
            d = t.add(odm.HashTable())
            d['foo'] = 'ciao'
        yield t.on_result
        yield self.async.assertEqual(d.size(), 1)
        yield self.async.assertEqual(d['foo'], 'ciao')
        yield self.async.assertRaises(KeyError, d.pop, 'bla')
        yield self.async.assertEqual(d.pop('bla', 56), 56)
        self.assertRaises(TypeError, d.pop, 'bla', 1, 2)
        yield self.async.assertEqual(d.pop('foo'), 'ciao')
        yield self.async.assertEqual(len(d), 0)
        
    def testGet(self):
        session = self.session()
        with session.begin() as t:
            h = t.add(odm.HashTable())
            h['bla'] = 'foo'
            h['bee'] = 3
        yield t.on_result
        result = yield multi_async((h['bla'], h.get('bee'), h.get('ggg'), h.get('ggg', 1)))
        self.assertEqual(result, ['foo', 3, None, 1])
        result = yield self.async.assertRaises(KeyError, lambda : h['gggggg'])
