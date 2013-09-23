from stdnet import odm
from stdnet.utils import test

class TestModel(test.TestCase):
    multipledb = False
    
    def test_create(self):
        User = odm.create_model('User', 'name', 'email', 'name')
        self.assertTrue(isinstance(User, odm.ModelType))
        self.assertEqual(User._meta.attributes, ('name', 'email'))
        
    def test_create_name(self):
        User = odm.create_model('UserBase', 'name', 'email', 'name',
                                abstract=True)
        self.assertEqual(User.__name__, 'UserBase')
        self.assertTrue(User._meta.abstract)
        self.assertRaises(AttributeError, User._meta.pkname)
        
    def test_init(self):
        User = odm.create_model('User', 'name', 'email')
        user = User(name='luca')
        self.assertEqual(user.name, 'luca')
        self.assertEqual(user.email, None)
        self.assertRaises(ValueError, User, bla='foo')
        
    def test_init_args(self):
        User = odm.create_model('User', 'name', 'email')
        user = User('luca')
        self.assertEqual(user.name, 'luca')
        self.assertEqual(user.email, None)
        user = User('bla', 'bla@foo')
        self.assertEqual(user.name, 'bla')
        self.assertEqual(user.email, 'bla@foo')
        self.assertRaises(ValueError, User, 'foo', 'jhjh', 'gjgj')
        
    def test_router(self):
        models = odm.Router()
        User = odm.create_model('User', 'name', 'email', 'name')
        models.register(User)
        self.assertEqual(models.user.model, User)