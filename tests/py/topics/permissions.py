from examples.permissions import User, Group, Role, InstanceRole,\
                                 register_for_permissions, authenticated_query
from examples.data import data_generator

from stdnet import odm
from stdnet.utils import test, populate, zip

read = 10
comment = 15
create = 20
update = 30
delete = 40

class MyModel(odm.StdModel):
    pass


class namesGenerator(data_generator):
    
    def generate(self, **kwargs):
        group_size = self.size // 2
        self.usernames = populate('string', self.size, min_len=5, max_len=20)
        self.passwords = populate('string', self.size, min_len=7, max_len=20)
        self.groups = populate('string', group_size, min_len=5, max_len=10)


class TestPermissions(test.CleanTestCase):
    models = (User, Group, Role, InstanceRole, MyModel)
    data_cls = namesGenerator

    @classmethod
    def setUpClass(cls):
        yield super(TestPermissions, cls).setUpClass()
        cls.data = cls.data_cls(size=cls.size)
        register_for_permissions(MyModel)
        
    def setUp(self):
        self.register()
        d = self.data
        with User.objects.transaction() as t:
            for username, password in zip(d.usernames, d.passwords):
                t.add(User(username=username, password=password))
        yield t.on_result
                
    def test_model_meta(self):
        self.assertTrue('user' in MyModel._meta.dfields)
        
    def test_user(self):
        users = yield User.objects.query().all()
        self.assertTrue(users)
        
    def test_create_without_role(self):
        users = yield User.objects.query().all()
        user1 = users[0]
        user2 = users[1]
        instance1 = MyModel(user=user1).save()
        instance2 = MyModel(user=user1).save()
        instance3 = MyModel(user=user1).save()
        instance4 = MyModel(user=user2).save()
        query = MyModel.objects.query()
        qs = authenticated_query(query, user1, read)
        self.assertEqual(qs.count(), 3)
        