import time
from datetime import date, datetime

import stdnet
from stdnet import test, orm
from stdnet.utils import nested_json_value, date2timestamp, timestamp2date

from examples.models import Statistics3


class TestUtils(test.TestCase):
    model = Statistics3
    
    def testNestedJasonValue(self):
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