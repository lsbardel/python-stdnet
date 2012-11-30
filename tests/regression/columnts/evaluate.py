from datetime import date

from stdnet.utils import test
from stdnet.apps.columnts import ColumnTS

class TestEvaluate(test.TestCase):

    def testSimple(self):
        session = self.session()
        ts = session.add(ColumnTS(id = 'goog'))
        self.assertEqual(ts.evaluate('return self:length()'), 0)
        ts.update({date(2012,5,15): {'open':605},
                   date(2012,5,16): {'open':617}})
        self.assertEqual(ts.evaluate('return self:length()'), 2)
        self.assertEqual(ts.evaluate('return self:fields()'), [b'open'])
        #Return the change from last open with respect prevois open
        change = "return self:rank_value(-1,'open')-"\
                 "self:rank_value(-2,'open')"
        self.assertEqual(ts.evaluate(change), 12)
