import time
from datetime import date, datetime

import stdnet
from stdnet import test, orm
from stdnet.utils import date2timestamp, timestamp2date,\
                            addmul_number_dicts, grouper

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
            nested_json_value(obj,'data__folder1__folder11',orm.JSPLITTER),1)
        self.assertEqual(\
            nested_json_value(obj,'data__folder1__folder12',orm.JSPLITTER),2)
        self.assertEqual(\
            nested_json_value(obj,'data__folder1',orm.JSPLITTER),'home')
        
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
        
    def testGrouper(self):
        r = grouper(2,[1,2,3,4,5,6,7])
        self.assertFalse(hasattr(r,'__len__'))
        self.assertEqual(list(r),[(1,2),(3,4),(5,6),(7,None)])
        r = grouper(3,'abcdefg','x')
        self.assertFalse(hasattr(r,'__len__'))
        self.assertEqual(list(r),[('a','b','c'),('d','e','f'),('g','x','x')])
    
    