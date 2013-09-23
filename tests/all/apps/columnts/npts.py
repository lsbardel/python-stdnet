import os

from stdnet import odm
from stdnet.utils import test, encoders
try:
    from stdnet.apps.columnts import npts
    from dynts import tsname
    
    nptsColumnTS = npts.ColumnTS
    
    class ColumnTimeSeriesNumpy(odm.StdModel):
        ticker = odm.SymbolField(unique = True)
        data = npts.ColumnTSField()
    
except ImportError:
    nptsColumnTS = None
    ColumnTimeSeriesNumpy = None
    
from . import main


skipUnless = test.unittest.skipUnless


@skipUnless(nptsColumnTS, 'Requires dynts') 
class TestDynTsIntegration(main.TestOperations):
    ColumnTS = nptsColumnTS 
            
    def testGetFields(self):
        ts1 = self.create()
        ts = ts1.irange()
        self.assertEqual(ts.count(),6)
        d1,v1 = ts1.front()
        d2,v2 = ts1.back()
        self.assertTrue(d2>d1)
        
    def testEmpty(self):
        session = self.session()
        ts1 = session.add(self.ColumnTS())
        ts = ts1.irange()
        self.assertEqual(len(ts),0)
        self.assertFalse(ts1.front())
        self.assertFalse(ts1.back())
    
    def testgetFieldInOrder(self):
        ts1 = self.create()
        ts = ts1.irange(fields = ('a','b','c'))
        self.assertEqual(ts.count(), 3)
        self.assertEqual(ts.name, tsname('a','b','c'))
        
    def testgetItem(self):
        ts1 = self.create()
        dates = list(ts1)
        N = len(dates)
        self.assertTrue(N)
        n = N//2
        dte = dates[n]
        v = ts1[dte]
        