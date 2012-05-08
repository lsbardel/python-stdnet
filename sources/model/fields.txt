.. _model-field:

.. module:: stdnet.odm

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
    
    This is an added bonus which can be of high importance when prototyping.
 

.. _fieldbaseclasses:

Base Classes
==============================

The :class:`Field` class is the base class for all fields and contains all
the attribute and method definitions. Derived classes only implement a handful
of methods.

.. _fieldbase:

Field
~~~~~~~~~~~~~~

.. autoclass:: Field
   :members:
   :member-order: bysource

.. _atomfield:

AtomField
~~~~~~~~~~~~~~~

.. autoclass:: AtomField
   :members:
   :member-order: bysource


StructureField
~~~~~~~~~~~~~~~

.. autoclass:: StructureField
   :members:
   :member-order: bysource
   

.. _atomfields:

Atom Fields
===========================

Atom Fields derived from :class:`AtomField` and, as the name says,
they represent the simplest data in a model. Their representation in python,
is one of ``bytes``, ``strings``, ``numbers`` or ``dates``.

IntegerField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: IntegerField
   :members:
   :member-order: bysource


BooleanField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: BooleanField
   :members:
   :member-order: bysource

   
AutoField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: AutoField
   :members:
   :member-order: bysource


FloatField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: FloatField
   :members:
   :member-order: bysource   


SymbolField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: SymbolField
   :members:
   :member-order: bysource
   
   
CharField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: CharField
   :members:
   :member-order: bysource
 
   
ByteField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ByteField
   :members:
   :member-order: bysource
 

.. _datefield:
   
DateField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: DateField
   :members:
   :member-order: bysource
   
   
.. _datetimefield:
   
DateTimeField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: DateTimeField
   :members:
   :member-order: bysource
   
   
CompositeIdField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: CompositeIdField
   :members:
   :member-order: bysource

.. _objectfields:
   
Object Type Fields
==========================   

JSONField
~~~~~~~~~~~~~~~~~~~~

.. autoclass:: JSONField
   :members:
   :member-order: bysource
   
   
PickleObjectField
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: PickleObjectField
   :members:
   :member-order: bysource
   

ModelField
~~~~~~~~~~~~~~~

.. autoclass:: ModelField
   :members:
   :member-order: bysource
   

.. _relatedfields:
   
Related Fields
==========================

.. _foreignkey:
   
ForeignKey
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ForeignKey
   :members:
   :member-order: bysource
   
   
.. _manytomany:
   
ManyToManyField
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: ManyToManyField
   :members:
   :member-order: bysource

   
.. _model-field-structure:

Data-structure Fields
============================

These fields are remote data-structures such as list, sets and hash tables.
They can be bound to models so that
many-to-many objects relationship can be established. All the data-structure
fields derives from :class:`StructureField`.


ListField
~~~~~~~~~~~~~~

.. autoclass:: ListField
   :members:
   :member-order: bysource
   

SetField
~~~~~~~~~~~~~

.. autoclass:: SetField
   :members:
   :member-order: bysource
   
   
HashField
~~~~~~~~~~

.. autoclass:: HashField
   :members:
   :member-order: bysource


.. _model-field-descriptors:

Descriptors
===================

.. autoclass:: LazyForeignKey
   :members:
   :member-order: bysource   
   
   
.. autoclass:: StructureFieldProxy
   :members:
   :member-order: bysource