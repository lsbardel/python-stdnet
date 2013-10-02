from stdnet.utils import test

from tests.all.multifields.struct import MultiFieldMixin

from examples.tsmodels import ColumnTimeSeries

from .npts import ColumnTimeSeriesNumpy, skipUnless


class TestColumnTSField(MultiFieldMixin, test.TestCase):
    model = ColumnTimeSeries

    def testModel(self):
        meta = self.model._meta
        self.assertTrue(len(meta.multifields),1)
        m = meta.multifields[0]
        self.assertEqual(m.name,'data')
        self.assertTrue(isinstance(m.value_pickler, encoders.Double))


@skipUnless(ColumnTimeSeriesNumpy, 'Requires stdnet-redis and dynts')
class TestColumnTSField(TestColumnTSField):
    model = ColumnTimeSeriesNumpy

    def setUp(self):
        self.register()

    def testMeta(self):
        meta = self.model._meta
        self.assertTrue(len(meta.multifields),1)
        m = meta.multifields[0]
        self.assertEqual(m.name, 'data')
        self.assertTrue(isinstance(m.value_pickler, encoders.Double))
