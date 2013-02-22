from stdnet import odm
from stdnet.utils import test

from .base import StructMixin


class TestHash(StructMixin, test.CleanTestCase):
    structure = odm.HashTable
    name = 'hashtable'
    
    def createOne(self, session):
        h = session.add(odm.HashTable())
        h['bla'] = 'foo'
        h['pluto'] = 3
        return h
        
    def testNoTransaction(self):
        session = self.session()
        d = session.add(odm.HashTable())
        d['bla'] = 5676
        self.assertEqual(d.size(),1)
        self.assertEqual(d['bla'],5676)
        
    def testPop(self):
        session = self.session()
        with session.begin():
            d = session.add(odm.HashTable())
            d['foo'] = 'ciao'
        self.assertEqual(d.size(),1)
        self.assertEqual(d['foo'],'ciao')
        self.assertRaises(KeyError, lambda : d.pop('bla'))
        self.assertEqual(d.pop('bla',56),56)
        self.assertRaises(TypeError, lambda : d.pop('bla',1,2))
        self.assertEqual(d.pop('foo'),'ciao')
        self.assertEqual(len(d),0)
        
    def testGet(self):
        session = self.session()
        with session.begin():
            h = session.add(odm.HashTable())
            h['bla'] = 'foo'
            h['bee'] = 3
        
        self.assertEqual(h['bla'],'foo')
        self.assertEqual(h.get('bee'),3)
        self.assertEqual(h.get('ggg'),None)
        self.assertEqual(h.get('ggg',1),1)
        self.assertRaises(KeyError, lambda : h['gggggg'])
