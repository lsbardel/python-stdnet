'''Test the JSON serializer'''
from stdnet import odm

from examples.data import FinanceTest, Fund

from . import base


class TestFinanceJSON(base.SerializerMixin, FinanceTest):
    serializer = 'json'

    def testTwoModels(self):
        models = self.mapper
        s = yield self.dump()
        d = s.data[0]
        self.assertEqual(d['model'], str(self.model._meta))
        all = yield models.fund.query().sort_by('id').all()
        s.dump(all)
        self.assertEqual(len(s.data), 2)


class TestLoadFinanceJSON(base.LoadSerializerMixin, FinanceTest):
    serializer = 'json'


