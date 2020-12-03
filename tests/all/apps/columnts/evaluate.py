from datetime import date

from stdnet.apps.columnts import ColumnTS
from stdnet.utils import test

from .main import ColumnMixin


class TestEvaluate(ColumnMixin, test.TestCase):
    def test_simple(self):
        ts = self.empty()
        l = yield ts.evaluate("return self:length()")
        self.assertEqual(l, 0)
        yield ts.update(
            {date(2012, 5, 15): {"open": 605}, date(2012, 5, 16): {"open": 617}}
        )
        yield self.async.assertEqual(ts.evaluate("return self:length()"), 2)
        yield self.async.assertEqual(ts.evaluate("return self:fields()"), [b"open"])
        # Return the change from last open with respect previous open
        change = "return self:rank_value(-1,'open')-" "self:rank_value(-2,'open')"
        change = yield ts.evaluate(change)
        self.assertEqual(change, 12)
