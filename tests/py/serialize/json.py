'''Test the JSON serializer'''
from stdnet import odm

from examples.data import FinanceTest, Fund

from . import base


class TestFinanceJSON(base.SerializerMixin, FinanceTest):
    serializer = 'json'

    def testTwoModels(self):
        s = yield self.dump()
        d = s.data[0]
        self.assertEqual(d['model'], str(self.model._meta))
        all = yield Fund.objects.query().sort_by('id').all()
        s.dump(all)
        self.assertEqual(len(s.data), 2)

    def testModelToSerialize(self):
        all = list(odm.all_models_sessions(self.models))
        self.assertEqual(len(all), 3)
        for m, session in all:
            self.assertNotEqual(session, None)


class TestLoadFinanceJSON(base.LoadSerializerMixin, FinanceTest):
    serializer = 'json'


