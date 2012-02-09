from datetime import date, timedelta

from stdnet.utils import encoders
from stdnet import test

try:
    import dynts
except ImportError:
    dynts = None
    
    
@test.unittest.skipUnless(dynts,'Requires dynts')
class DyntsTimeSerie(test.TestCase):
    
    def ts(self, name = None):
        from stdnet.apps.columnts.npts import TS
        return TS(id = name, pickler = encoders.DateConverter())
    
    def adddata(self):
        session = self.session()
        ts = session.add(self.ts())
        with ts.session.begin():
            ts.add(date.today(),{'bla':3})
            ts.add(date.today()-timedelta(days=1),{'foo':'pippo'})
        return ts
   
    def testSimple(self):
        ts = self.adddata()
        data = ts.irange()
        self.assertEqual(len(data),2)
        
            
            
        
    
    