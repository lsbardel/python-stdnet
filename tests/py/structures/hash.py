from pulsar import multi_async

from stdnet import odm
from stdnet.utils import test

from .base import StructMixin
        
        
class TestHash(StructMixin, test.TestCase):
    structure = odm.HashTable
    name = 'hashtable'
    
    def create_one(self):
        h = odm.HashTable()
        h['bla'] = 'foo'
        h['pluto'] = 3
        return h
        
    def testEmpty(self):
        with self.session().begin() as t:
            d = t.add(self.structure())
        yield t.on_result
        result = yield d.get('blaxxx', 3)
        self.assertEqual(result, 3)
        
    def testNoTransaction(self):
        session = self.session()
        d = odm.HashTable()
        d['bla'] = 5676
        self.assertFalse(d.session)
        yield session.add(d)
        yield self.async.assertEqual(d.size(), 1)
        yield self.async.assertEqual(d['bla'], 5676)
        
    def testPop(self):
        with self.session().begin() as t:
            d = t.add(odm.HashTable())
            d['foo'] = 'ciao'
        yield t.on_result
        yield self.async.assertEqual(d.size(), 1)
        yield self.async.assertEqual(d['foo'], 'ciao')
        yield self.async.assertRaises(KeyError, d.pop, 'bla')
        yield self.async.assertEqual(d.pop('xxx', 56), 56)
        self.assertRaises(TypeError, d.pop, 'xxx', 1, 2)
        yield self.async.assertEqual(d.pop('foo'), 'ciao')
        yield self.async.assertEqual(d.size(), 0)
        
    def testGet(self):
        session = self.session()
        with session.begin() as t:
            h = t.add(odm.HashTable())
            h['baba'] = 'foo'
            h['bee'] = 3
            self.assertEqual(len(h.cache.toadd), 2)
        yield t.on_result
        result = yield multi_async((h['baba'], h.get('bee'), h.get('ggg'), h.get('ggg', 1)))
        self.assertEqual(result, ['foo', 3, None, 1])
        result = yield self.async.assertRaises(KeyError, lambda : h['gggggg'])
