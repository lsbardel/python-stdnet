import time

import stdnet
from stdnet import test, orm
from stdnet.utils.jsontools import nested_json_value

from examples.models import Statistics3


class TestUtils(test.TestModelBase):
    model = Statistics3
    
    def testNestedJasonValue(self):
        data = {'data':1000,
                'folder1':{'folder11':1,
                           'folder12':2,
                           '':'home'}}
        obj = self.model(name='foo',data=data).save()
        obj = self.model.objects.get(id = obj.id)
        self.assertEqual(\
            nested_json_value(obj,'data__folder1__folder11',orm.JSPLITTER),1)
        self.assertEqual(\
            nested_json_value(obj,'data__folder1__folder12',orm.JSPLITTER),2)
        self.assertEqual(\
            nested_json_value(obj,'data__folder1',orm.JSPLITTER),'home')