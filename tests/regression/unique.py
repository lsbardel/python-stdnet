from random import randint

from stdnet import test, orm
from stdnet.utils import populate, zip, range

from examples.models import SimpleModel


class TestUnique(test.TestCase):
    model = SimpleModel
    
    def testSimple(self):
        session = self.session()
        m = session.add(self.model(code = 'me'))
        self.assertEqual(m.id,1)
        # Create another one
        m = session.add(self.model(code = 'me'))
        self.assertEqual(m.id,1)
    
