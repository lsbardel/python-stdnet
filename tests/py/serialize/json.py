'''Test the JSON serializer'''
from stdnet import odm
from stdnet.utils import test

from examples.data import FinanceTest, DataTest
from examples.models import Instrument, Fund, Position, Folder, PortfolioView

from .base import SerializerMixin


class TestFinanceJSON(FinanceTest, SerializerMixin):
    serializer = 'json'
    models = (Instrument, Fund, Position, Folder)

    def setUp(self):
        self.register()

    def testTwoModels(self):
        s = self.testDump()
        self.assertEqual(len(s.data),1)
        d = s.data[0]
        self.assertEqual(d['model'],str(self.model._meta))
        s.serialize(Fund.objects.query().sort_by('id'))
        self.assertEqual(len(s.data), 2)

    def testModelToSerialize(self):
        all = list(odm.all_models_sessions(self.models))
        self.assertEqual(len(all), 6)
        for m, session in all:
            if m == PortfolioView:
                self.assertEqual(session, None)
            else:
                self.assertNotEqual(session, None)



