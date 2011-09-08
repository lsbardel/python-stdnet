.. _model-field:

.. module:: stdnet.orm

============================
Fields API
============================

The most important part of a model, and the only required part of a model,
is the list of database fields it defines. Fields are specified by class attributes.
They are the equivalent to django Fields and therefore
the equivalent of columns in a traditional relational databases.

.. note:: There is an important difference between stdnet fields and fields
    in a traditional relational database. Stdnet fields can be added, removed or
    changed without requiring any time consuming database migration!
    
    This is an added bonus which can be of high importance when prototyping but
    it can lead to loss of data if a user abuses it.
 

.. _fieldbaseclasses:

Base Classes
==============================

The :class:`Field` class is the base class for all fields and contains all
the attribute and method definitions. Derived classes only implement a handful
of methods.

.. _fieldbase:

Field
~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.Field
   :members:
   :member-order: bysource

.. _atomfield:

AtomField
~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.AtomField
   :members:
   :member-order: bysource


MultiField
~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.MultiField
   :members:
   :member-order: bysource
   

.. _atomfields:

Atom Fields
===========================

Atom Fields derived from :class:`stdnet.orm.AtomField` and, as the name says,
they represent the simplest data in a model. Their representation in python,
is one of ``bytes``, ``strings``, ``numbers`` or ``dates``.

IntegerField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.IntegerField
   :members:
   :member-order: bysource


BooleanField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.BooleanField
   :members:
   :member-order: bysource

   
AutoField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.AutoField
   :members:
   :member-order: bysource


FloatField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.FloatField
   :members:
   :member-order: bysource   


SymbolField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.SymbolField
   :members:
   :member-order: bysource
   
   
CharField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.CharField
   :members:
   :member-order: bysource
 
   
ByteField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.ByteField
   :members:
   :member-order: bysource
 

.. _datefield:
   
DateField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.DateField
   :members:
   :member-order: bysource
   
   
.. _datetimefield:
   
DateTimeField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.DateTimeField
   :members:
   :member-order: bysource
   

.. _objectfields:
   
Object Type Fields
==========================

.. _foreignkey:
   
ForeignKey
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.ForeignKey
   :members:
   :member-order: bysource
   

JSONField
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.JSONField
   :members:
   :member-order: bysource
   
   
PickleObjectField
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.PickleObjectField
   :members:
   :member-order: bysource
   

ModelField
~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.ModelField
   :members:
   :member-order: bysource
   
   
.. _model-field-structure:

Data-structure Fields
============================

These fields are remote data-structures such as list, sets and hash tables.
They can be bound to models so that
many-to-many objects relationship can be established. All the data-structure
fields derives from :class:`stdnet.orm.MultiField`.


ListField
~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.ListField
   :members:
   :member-order: bysource
   

SetField
~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.SetField
   :members:
   :member-order: bysource
   
   
HashField
~~~~~~~~~~

.. autoclass:: stdnet.orm.HashField
   :members:
   :member-order: bysource


ManyToManyField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.ManyToManyField
   :members:
   :member-order: bysource

   