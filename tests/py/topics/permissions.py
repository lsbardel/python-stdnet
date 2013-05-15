from examples.permissions import User, Group, Role, InstanceRole,\
                                 register_for_permissions, authenticated_query

from stdnet import odm
from stdnet.utils import test, zip

read = 10
comment = 15
create = 20
update = 30
delete = 40

class MyModel(odm.StdModel):
    pass


class NamesGenerator(test.DataGenerator):
    
    def generate(self):
        group_size = self.size // 2
        self.usernames = self.populate(min_len=5, max_len=20)
        self.passwords = self.populate(min_len=7, max_len=20)
        self.groups = self.populate(size=group_size, min_len=5, max_len=10)


class TestPermissions(test.TestWrite):
    models = (User, Group, Role, InstanceRole, MyModel)
    data_cls = NamesGenerator
    
    @classmethod
    def after_setup(cls):
        register_for_permissions(MyModel)
                        
    def setUp(self):
        d = self.data
        models = self.mapper
        with models.session().begin() as t:
            for username, password in zip(d.usernames, d.passwords):
                t.add(models.user(username=username, password=password))
        yield t.on_result
                
    def test_model_meta(self):
        self.assertTrue('user' in MyModel._meta.dfields)
        
    def test_user(self):
        models = self.mapper
        users = yield models.user.all()
        self.assertTrue(users)
        
    def test_create_without_role(self):
        models = self.mapper
        session = models.session()
        users = yield models.user.all()
        user1 = users[0]
        user2 = users[1]
        with session.begin() as t:
            instance1 = t.add(models.mymodel(user=user1))
            instance2 = t.add(models.mymodel(user=user1))
            instance3 = t.add(models.mymodel(user=user1))
            instance4 = t.add(models.mymodel(user=user2))
        yield t.on_result
        query = models.mymodel.query()
        qs = authenticated_query(query, user1, read)
        yield self.async.assertEqual(qs.count(), 3)
        