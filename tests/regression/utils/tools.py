import time
from datetime import date, datetime

import stdnet
from stdnet import test, odm
from stdnet.utils import encoders, to_bytes, to_string
from stdnet.utils import date2timestamp, timestamp2date,\
                            addmul_number_dicts, grouper,\
                            _format_int, populate

from examples.models import Statistics3


class TestUtils(test.TestCase):
    model = Statistics3
    
    def __testNestedJasonValue(self):
        data = {'data':1000,
                'folder1':{'folder11':1,
                           'folder12':2,
                           '':'home'}}
        session = self.session()
        with session.begin():
            session.add(self.model(name='foo',data=data))
        obj = session.query(self.model).get(id = 1)
        self.assertEqual(\
            nested_json_value(obj,'data__folder1__folder11',odm.JSPLITTER),1)
        self.assertEqual(\
            nested_json_value(obj,'data__folder1__folder12',odm.JSPLITTER),2)
        self.assertEqual(\
            nested_json_value(obj,'data__folder1',odm.JSPLITTER),'home')
        
    def test_date2timestamp(self):
        t1 = datetime.now()
        ts1 = date2timestamp(t1)
        self.assertEqual(timestamp2date(ts1),t1)
        t1 = date.today()
        ts1 = date2timestamp(t1)
        t = timestamp2date(ts1)
        self.assertEqual(t.date(),t1)
        self.assertEqual(t.hour,0)
        self.assertEqual(t.minute,0)
        self.assertEqual(t.second,0)
        self.assertEqual(t.microsecond,0)
        
    def test_addmul_number_dicts(self):
        d1 = {'bla': 2.5, 'foo': 1.1}
        d2 = {'bla': -2, 'foo': -0.3}
        r = addmul_number_dicts((2,d1),(-1,d2))
        self.assertEqual(len(r),2)
        self.assertEqual(r['bla'],7)
        self.assertEqual(r['foo'],2.5)
        
    def test_addmul_number_dicts2(self):
        d1 = {'bla': 2.5, 'foo': 1.1}
        d2 = {'bla': -2, 'foo': -0.3, 'moon': 8.5}
        r = addmul_number_dicts((2,d1),(-1,d2))
        self.assertEqual(len(r),2)
        self.assertEqual(r['bla'],7)
        self.assertEqual(r['foo'],2.5)
        
    def test_addmul_nested_dicts(self):
        d1 = {'bla': {'bla1': 2.5}, 'foo': 1.1}
        d2 = {'bla': {'bla1': -2}, 'foo': -0.3, 'moon': 8.5}
        r = addmul_number_dicts((2,d1),(-1,d2))
        self.assertEqual(len(r),2)
        self.assertEqual(r['bla']['bla1'],7)
        self.assertEqual(r['foo'],2.5)
    
    
class testFunctions(test.TestCase):
    
    def testGrouper(self):
        r = grouper(2,[1,2,3,4,5,6,7])
        self.assertFalse(hasattr(r,'__len__'))
        self.assertEqual(list(r),[(1,2),(3,4),(5,6),(7,None)])
        r = grouper(3,'abcdefg','x')
        self.assertFalse(hasattr(r,'__len__'))
        self.assertEqual(list(r),[('a','b','c'),('d','e','f'),('g','x','x')])
        
    def testFormatInt(self):
        self.assertEqual(_format_int(4500),'4,500')
        self.assertEqual(_format_int(4500780),'4,500,780')
        self.assertEqual(_format_int(500),'500')
        self.assertEqual(_format_int(-780),'-780')
        self.assertEqual(_format_int(-4500780),'-4,500,780')
        
    def testPopulateIntegers(self):
        data = populate('integer', size = 33)
        self.assertEqual(len(data),33)
        for d in data:
            self.assertTrue(isinstance(d,int))
            
    def testAbstarctEncoder(self):
        e = encoders.Encoder()
        self.assertRaises(NotImplementedError , e.dumps, 'bla')
        self.assertRaises(NotImplementedError , e.loads, 'bla')
        
    def test_to_bytes(self):
        self.assertEqual(to_bytes(b'ciao'),b'ciao')
        b = b'perch\xc3\xa9'
        u = b.decode('utf-8')
        l = u.encode('latin')
        self.assertEqual(to_bytes(b,'latin'),l)
        self.assertEqual(to_string(l,'latin'),u)
        