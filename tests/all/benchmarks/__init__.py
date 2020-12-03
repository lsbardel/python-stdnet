import os

from examples.data import CCYS_TYPES, INSTS_TYPES, finance_data
from examples.models import Fund, Instrument, PortfolioView, Position, UserDefaultView

from stdnet.utils import test


class Benchmarks(test.TestWrite):
    __benchmark__ = True
    data_cls = finance_data
    models = (Instrument, Fund, Position)

    def test_create(self):
        session = yield self.data.create(self)
