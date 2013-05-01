from stdnet.utils import test
from stdnet import getdb

try:
    from examples.sql import User
except ImportError:
    User = None


@test.unittest.skipUnless(User, 'Requires SqlAlchemy')
class TestSqlManager(test.TestCase):
    
    @classmethod
    def after_setup(cls):
        cls.engine = getdb('sqlite://')
        cls.mapper.register(User, cls.engine)
        cls.mapper.create_all()
        
    def test_registration(self):
        manager = self.mapper[User]
        self.assertEqual(manager.backend, self.engine)
        self.assertEqual(manager.model, User)
        self.assertEqual(manager, self.mapper.user)
        
    def test_session(self):
        models = self.mapper
        session = models.user.session()
        self.assertTrue(session)
        
    def test_create(self):
        models = self.mapper
        user = models.user(fullname='Pippo', email='pippo@pippo.com')
        self.assertEqual(user.fullname, 'Pippo')
        self.assertEqual(user.email, 'pippo@pippo.com')
        self.assertEqual(user.password, None)
        self.assertEqual(user.id, None)
        
    def test_commit(self):
        models = self.mapper
        user = yield models.user.new(fullname='Pippo', email='pippo@pippo.com')
        self.assertTrue(user.id)
        self.assertEqual(user.fullname, 'Pippo')
        self.assertEqual(user.email, 'pippo@pippo.com')
        
    
    