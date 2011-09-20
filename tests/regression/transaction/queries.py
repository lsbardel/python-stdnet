import random

import stdnet
from stdnet import test, orm

from regression.finance.tests import BaseFinance, Instrument, Fund

__all__ = ['TestTransactionQuery']


class TestTransactionQuery(BaseFinance):
    
    def testDoubleQuery(self):
        with orm.transaction(Instrument,Fund) as t:
            insts = Instrument.objects.all(transaction = t)
            funds = Fund.objects.all(transaction = t)
    