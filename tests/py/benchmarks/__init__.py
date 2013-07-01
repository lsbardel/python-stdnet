import os

from stdnet.utils import test

from examples.models import Instrument, Fund, Position, PortfolioView,\
                             UserDefaultView
from examples.data import finance_data, INSTS_TYPES, CCYS_TYPES


class Benchmarks(test.TestWrite):
    __benchmark__ = True
    data_cls = finance_data
    models = (Instrument, Fund, Position)
    
    def test_create(self):
        session = yield self.data.create(self)
        
        