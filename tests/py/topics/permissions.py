from random import choice
from examples.permissions import User, Group, Role, Permission

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
        


class TestPermissions(test.TestCase):
    models = (User, Group, Role, Permission, MyModel)
    data_cls = NamesGenerator
    
    @classmethod
    def after_setup(cls):
        d = cls.data
        models = cls.mapper
        groups = []
        groups.append(models.group.create_user(username='stdnet',
                                               can_login=False))
        for username, password in zip(d.usernames, d.passwords):
            groups.append(models.group.create_user(username=username,
                                                   password=password))
        yield cls.multi_async(groups)
        session = models.session()
        groups = yield session.query(Group).all()
        with models.session().begin() as t:
            for group in groups: 
                group.create_role('family') # create the group-family role
                group.create_role('friends') # create the group-friends role
        yield t.on_result
    
    def random_group(self, *excludes):
        if excludes:
            name = choice(list(set(self.data.usernames)-set(excludes)))
        else:
            name = choice(self.data.usernames)
        return self.mapper.group.get(name=name)
        
    def test_group_query(self):
        groups = self.mapper.group
        cache = groups._meta.dfields['user'].get_cache_name()
        groups = yield groups.all()
        for g in groups:
            self.assertTrue(hasattr(g, cache))
            self.assertEqual(g.user.username, g.name)
        
    def test_create_role(self, name=None):
        # Create a new role
        name = name or self.data.random_string()
        models = self.mapper
        group = yield self.random_group()
        role = yield group.create_role(name) # add a random role
        self.assertEqual(role.name, name)
        self.assertEqual(role.owner, group)
        permission = yield role.add_permission(MyModel, read)
        self.assertEqual(permission.model_type, MyModel)
        self.assertEqual(permission.object_pk, '')
        self.assertEqual(permission.operation, read)
        #
        # the role should have only one permission
        permissions = yield role.permissions.all()
        self.assertTrue(len(permissions)>=1)
        yield role
        
    def test_role_assignto_group(self):
        role = yield self.test_create_role()
        group = yield self.random_group(role.owner.name)
        # we add role to group
        # return the Subject Assignment (SA) for this role-subject link
        sa = yield role.assignto(group)
        self.assertEqual(sa.role, role)
        self.assertEqual(sa.group, group)
        #
        # group has a new role
        roles = yield group.roles.all()
        self.assertTrue(role in roles)
        