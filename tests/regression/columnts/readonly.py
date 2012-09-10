from datetime import date

from stdnet import test
from stdnet.apps.columnts import ColumnTS

nan = float('nan')

class TestReadOnly(test.TestCase):

    def testInfoSimple(self):
        session = self.session()
        ts = session.add(ColumnTS(id='goog'))
        info = ts.info()
        self.assertEqual(info['size'], 0)
        self.assertFalse('start' in info)
        d1 = date(2012, 5, 15)
        d2 = date(2012, 5, 16)
        ts.update({d1: {'open':605},
                   d2: {'open':617}})
        info = ts.info()
        self.assertEqual(info['size'], 2)
        self.assertEqual(info['fields']['open']['missing'], 0)
        self.assertEqual(info['start'].date(), d1)
        self.assertEqual(info['stop'].date(), d2)
        d3 = date(2012,5,14)
        d4 = date(2012,5,13)
        ts.update({d3: {'open':nan,'close':607},
                   d4: {'open':nan,'close':nan}})
        info = ts.info()
        self.assertEqual(info['size'], 4)
        self.assertEqual(info['start'].date(), d4)
        self.assertEqual(info['stop'].date(), d2)
        self.assertEqual(info['fields']['open']['missing'], 2)
        self.assertEqual(info['fields']['close']['missing'], 3)


    def __test(self):
        ts.update({date(2012,5,15): {'open':605},
                   date(2012,5,16): {'open':617}})
        self.assertEqual(ts.evaluate('return self:length()'), 2)
        self.assertEqual(ts.evaluate('return self:fields()'), [b'open'])
        #Return the change from last open with respect prevois open
        change = "return self:rank_value(-1,'open')-"\
                 "self:rank_value(-2,'open')"
        self.assertEqual(ts.evaluate(change), 12)
