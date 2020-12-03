"""Test the CSV serializer"""
from examples.data import FinanceTest, Fund

from stdnet import odm

from . import base


class TestFinanceCSV(base.SerializerMixin, FinanceTest):
    serializer = "csv"

    def testTwoModels(self):
        models = self.mapper
        s = yield self.dump()
        self.assertEqual(len(s.data), 1)
        funds = yield models.fund.all()
        self.assertRaises(ValueError, s.dump, funds)
        self.assertEqual(len(s.data), 1)

    def testLoadError(self):
        s = yield self.dump()
        self.assertRaises(ValueError, s.load, self.mapper, "bla")


class TestLoadFinanceCSV(base.LoadSerializerMixin, FinanceTest):
    serializer = "csv"
