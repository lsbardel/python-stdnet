'''This example is manages Users, Groups and Permissions.

A Group is always owned by a User but several user can be part of that Group.
Lets consider a website where we need to manage permissions and be able to query
efficiently only on instances for which a Given user has enough permissions.

The first step is to register a model, lets assume one called ``MyModel``,
with the permission engine:

    register_for_permissions(MyModel)
    
from now on every time a new instance of ``MyModel`` is created, it must
be given the user owning the instance. 

First we create the website User and efine a set of permissions::

    website = User(username='website').save()
    
    read = 10
    create = 20
    update = 30
    delete = 40
    
The higher the permission level the more restrictive is the algorithm.
We then create a group for all authenticated users::

    authenticated = Group(name='authenticated', user=website).save()
    
and another group called 'administrator':

    admin = Group(name='administrator', user=website).save()
    
We want all ``authenticated`` users to be able to `read` instances from
model ``MyModel``::

    role = Role(name='can read my model',
                permission_level=read,
                model=MyModel).save()
    role.groups.add(authenticated)
    

We want all ``admin`` users to be able to `update` instances from
model ``MyModel`` but not delete them::

    role = Role(name='can create/update my model',
                permission_level=update,
                model=MyModel).save()
    role.groups.add(admin)
    
When we create a new instance of ``MyModel`` we need to give the required role

    instance = MyModel(....., user=user).save()
    
    role.add(instance)
    
Lets assume we have a ``query`` on ``MyModel`` and we need all the instances
for ``user`` with permission ``level``:

    authenticated_query(query, user, level)
'''
from stdnet import odm

class User(odm.StdModel):
    username = odm.SymbolField(unique=True)
    password = odm.CharField(required=True, hidden=True)
    first_name = odm.CharField()
    last_name = odm.CharField()
    email = odm.CharField()
    is_active = odm.BooleanField(default=True)
    is_superuser = odm.BooleanField(default=False)

    def __unicode__(self):
        return self.username
    
    def permitted_query(self, query, permission_level):
        # the model for which we need permissions
        groups = self.groups.query()
        model = query.model
        permitted = ObjectPermission.objects.filter(model_type=model)
    
    
class Group(odm.StdModel):
    '''A group is always owned by a user but can be assigned to several
ather users via the Role through model'''
    id = odm.CompositeIdField('name', 'user')
    name = odm.SymbolField(unique=True)
    user = odm.ForeignKey(User)
    #
    users = odm.ManyToManyField(User, related_name='groups')
    
    
class Role(odm.StdModel):
    '''A role is a permission level which can be assigned'''
    id = odm.CompositeIdField('name', 'model')
    name = odm.SymbolField()
    model_type = odm.ModelField()
    level = odm.IntegerField(default=0)
    #
    groups = odm.ManyToManyField(Group, related_name='roles')
    
    def add_instance(self, instance):
        if not isinstance(instance, self.model_type):
            raise ValueError
        with InstanceRole.objects.begin() as t:
            t.delete(InstanceRole.objects.filter(role=self,
                                                 object_id=instance.id))
            t.add(InstanceRole(role=self, object_id=instance.id))


class InstanceRole(odm.StdModel):
    id = odm.CompositeIdField('object_id', 'role')
    object_id = odm.SymbolField()
    role = odm.ForeignKey(Role)


def add_role(instance, permission_level, ):
    '''Add permission level to an instance of a model registered with
permissions'''

def authenticated_query(query, user, level):
    owner_query = query.filter(user=user)
    # all roles for the query model with appropriate permission level
    roles = Role.objects.filter(model_type=query.model, level__ge=level)
    # Now we need groups which have these roles
    groups = Role.groups.throughquery().filter(role=roles).get_field('group')
    # I need to know if user is in any of these groups
    if user.groups.filter(id=groups).count():
        # it is, lets get the model with permissions less
        # or equal permission level
        permitted = InstanceRole.objects.filter(role=roles).get_field('object_id')
        return owner_query.union(model.objects.filter(id=permitted))
    else:
        return owner_query
    

def register_for_permissions(model):
    if 'user' not in model._meta.dfields:
        user = odm.ForeignKey(User, related_name=model.__name__.lower())
        user.register_with_model('user', model)
    user = model._meta.dfields['user']
    if not isinstance(user, odm.ForeignKey) or user.relmodel != User:
        raise RuntimeError('user field of wrong type')
    
    
