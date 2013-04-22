from stdnet.utils import test

from examples.dynamo import Instrument


class PrimaryKey(test.TestCase):
    model = Instrument
    
    @classmethod
    def after_setup(cls):
        cls.register()
        
    def testCreate(self):
        m = yield Instrument(name='bla', ccy='EUR', type='foo').save()
        self.assertEqual(m.name, 'bla')