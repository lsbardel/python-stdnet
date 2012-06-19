from .main import TestColumnTSBase

class TestManipulate(TestColumnTSBase):
    
    def test_ipop_range(self):
        ts = self.create()
        N = ts.size()
        fields = ts.fields()
        self.assertEqual(len(fields), 6)
        dates, fields = ts.irange(N-2)
        dt, fs = ts.ipop_range(N-2)
        self.assertEqual(ts.size(), N-2)
        self.assertEqual(dates, dt)
        self.assertEqual(fields, fs)