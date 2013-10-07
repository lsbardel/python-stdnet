'''
This section is a practical application of ``stdnet`` for solving
role-based access control (RBAC). It is an approach for managing users
permissions on your application which could be a web-site, an organisation
and so forth.

Introduction
======================
There are five different elements to get familiar with this RBAC
implementation:

* **System**: this is your application/organization/web site.
* **Roles**: within a **system**, **roles** are created for various
  **operations**.
* **Permissions**: they represents the power to perform certain **operations**
  on a resource and are assigned to specific **roles** within a **system**.
* **Operations**: what can a user do with that permission, usually, ``read``
  ``write``, ``delete``.
* **Subjects**: these are the system users which are assigned particular
  **roles**.
  Permissions are not assigned directly to **subjects**, instead they are
  acquired through their roles.


The :class:`Permission` to perform certain certain **operations** are assigned
to specific **roles**.

This example is manages Users, Groups and Permissions.

.. rbac-roles

Roles
==============
In this implementation a role is uniquely identified by a ``name`` and
a ``owner``.

.. autoclass:: Role
   :members:
   :member-order: bysource


.. rbac-subject

Subjects
===============
The subject in this implementation is given by the :class:`Group`.
Therefore :ref:`roles <rbac-roles>` are assigned to :class:`Group`.
Since roles are implemented via the :class:`Role` model, the relationship
between :class:`Group` and :class:`Role` is obtained via a
:class:`stdnet.odm.ManyToManyField`.


.. autoclass:: Group
   :members:
   :member-order: bysource

The system user is implemented via the :class:`User`. Since roles are
always assigned to :class:`Group`, a :class:`User` obtains permissions via
the groups he belongs to.

.. autoclass:: User
   :members:
   :member-order: bysource

Design
==============
A :class:`Group` is always owned by a :class:`User` but several user can be
part of that Group. Lets consider a web-site where we need to manage
permissions and be able to query
efficiently only on instances for which a Given user has enough permissions.

The first step is to register a :class:`Model`, lets assume one called
``MyModel``, with the permission engine:

    register_for_permissions(MyModel)

from now on every time a new instance of ``MyModel`` is created, it must
be given the user owning the instance.

First we create the website User and define a set of permissions::

    website = models.user.new(username='website')

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
from inspect import isclass
from stdnet import odm, FieldError


class PermissionManager(odm.Manager):

    def for_object(self, object, **params):
        if isclass(object):
            qs = self.query(model_type=object)
        else:
            qs = self.query(model_type=object.__class__,
                            object_pk=object.pkvalue())
        if params:
            qs = qs.filter(**params)
        return qs


class GroupManager(odm.Manager):

    def query(self, session=None):
        '''Makes sure the :attr:`Group.user` is always loaded.'''
        return super(GroupManager, self).query(session).load_related('user')

    def check_user(self, username, email):
        '''username and email (if provided) must be unique.'''
        users = self.router.user
        avail = yield users.filter(username=username).count()
        if avail:
            raise FieldError('Username %s not available' % username)
        if email:
            avail = yield users.filter(email=email).count()
            if avail:
                raise FieldError('Email %s not available' % email)

    def create_user(self, username=None, email=None, **params):
        yield self.check_user(username, email)
        users = self.router.user
        user = yield users.new(username=username, email=email, **params)
        # Create the user group
        yield self.new(user=user, name=user.username)

    def permitted_query(self, query, group, operations):
        '''Change the ``query`` so that only instances for which
``group`` has roles with permission on ``operations`` are returned.'''
        session = query.session
        models = session.router
        user = group.user
        if user.is_superuser:   # super-users have all permissions
            return query
        roles = group.roles.query()
        roles = group.roles.query()  # query on all roles for group
        # The throgh model for Role/Permission relationship
        throgh_model = models.role.permissions.model
        models[throgh_model].filter(role=roles,
                                    permission__model_type=query.model,
                                    permission__operations=operations)

        # query on all relevant permissions
        permissions = router.permission.filter(model_type=query.model,
                                               level=operations)

        owner_query = query.filter(user=user)
        # all roles for the query model with appropriate permission level
        roles = models.role.filter(model_type=query.model, level__ge=level)
        # Now we need groups which have these roles
        groups = Role.groups.throughquery(
            session).filter(role=roles).get_field('group')
        # I need to know if user is in any of these groups
        if user.groups.filter(id=groups).count():
            # it is, lets get the model with permissions less
            # or equal permission level
            permitted = models.instancerole.filter(
                role=roles).get_field('object_id')
            return owner_query.union(model.objects.filter(id=permitted))
        else:
            return owner_query


class Subject(object):
    roles = odm.ManyToManyField('Role', related_name='subjects')

    def create_role(self, name):
        '''Create a new :class:`Role` owned by this :class:`Subject`'''
        models = self.session.router
        return models.role.new(name=name, owner=self)

    def assign(self, role):
        '''Assign :class:`Role` ``role`` to this :class:`Subject`. If this
:class:`Subject` is the :attr:`Role.owner`, this method does nothing.'''
        if role.owner_id != self.id:
            return self.roles.add(role)

    def has_permissions(self, object, group, operations):
        '''Check if this :class:`Subject` has permissions for ``operations``
on an ``object``. It returns the number of valid permissions.'''
        if self.is_superuser:
            return 1
        else:
            models = self.session.router
            # valid permissions
            query = models.permission.for_object(object, operation=operations)
            objects = models[models.role.permissions.model]
            return objects.filter(role=self.role.query(),
                                  permission=query).count()


class User(odm.StdModel):
    '''The user of a system. The only field required is the :attr:`username`.
which is also unique across all users.'''
    username = odm.SymbolField(unique=True)
    password = odm.CharField(required=False, hidden=True)
    first_name = odm.CharField()
    last_name = odm.CharField()
    email = odm.CharField()
    is_active = odm.BooleanField(default=True)
    can_login = odm.BooleanField(default=True)
    is_superuser = odm.BooleanField(default=False)

    def __unicode__(self):
        return self.username


class Group(odm.StdModel, Subject):
    id = odm.CompositeIdField('name', 'user')
    name = odm.SymbolField()
    '''Group name. If the group is for a signle user, it can be the
user username'''
    user = odm.ForeignKey(User)
    '''A group is always `owned` by a :class:`User`. For example the ``admin``
group for a website is owned by the ``website`` user.'''
    #
    users = odm.ManyToManyField(User, related_name='groups')
    '''The :class:`stdnet.odm.ManyToManyField` for linking :class:`User`
and :class:`Group`.'''
    roles = odm.ManyToManyField('Role', related_name='subjects')

    manager_class = GroupManager

    def __unicode__(self):
        return self.name


class Permission(odm.StdModel):
    '''A model which implements permission and operation within
this RBAC implementation.'''
    id = odm.CompositeIdField('model_type', 'object_pk', 'operation')
    '''The name of the role, for example, ``Editor`` for a role which can
    edit a certain :attr:`model_type`.'''
    model_type = odm.ModelField()
    '''The model (resource) which this permission refers to.'''
    operation = odm.IntegerField(default=0)
    '''The operation assigned to this permission.'''
    object_pk = odm.SymbolField(required=False)

    manager_class = PermissionManager

    def __unicode__(self):
        op = self.operation
        if self.object_pk:
            return '%s - %s - %s' % (self.model_type, self.object_pk, op)
        else:
            return '%s - %s' % (self.model_type, op)


class Role(odm.StdModel):
    '''A :class:`Role` is uniquely identified by its :attr:`name` and
:attr:`owner`.'''
    id = odm.CompositeIdField('name', 'owner')
    name = odm.SymbolField()
    '''The name of this role.'''
    owner = odm.ForeignKey(Group)
    '''The owner of this role-permission.'''
    permissions = odm.ManyToManyField(Permission, related_name='roles')
    '''the set of all :class:`Permission` assigned to this :class:`Role`.'''

    def __unicode__(self):
        return self.name

    def add_permission(self, resource, operation):
        '''Add a new :class:`Permission` for ``resource`` to perform an
``operation``. The resource can be either an object or a model.'''
        if isclass(resource):
            model_type = resource
            pk = ''
        else:
            model_type = resource.__class__
            pk = resource.pkvalue()
        p = Permission(model_type=model_type, object_pk=pk,
                       operation=operation)
        session = self.session
        if session.transaction:
            session.add(p)
            self.permissions.add(p)
            return p
        else:
            with session.begin() as t:
                t.add(p)
                self.permissions.add(p)
            return t.add_callback(lambda r: p)

    def assignto(self, subject):
        '''Assign this :class:`Role` to ``subject``.'''
        return subject.assign(self)


def register_for_permissions(model):
    if 'group' not in model._meta.dfields:
        group = odm.ForeignKey(Group, related_name=model.__name__.lower())
        group.register_with_model('group', model)
    group = model._meta.dfields['group']
    if not isinstance(group, odm.ForeignKey) or group.relmodel != Group:
        raise RuntimeError('group field of wrong type')
