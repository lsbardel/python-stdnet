.. _model-field:

.. module:: stdnet.orm

============================
Scalar Fields
============================

The most important part of a model, and the only required part of a model,
is the list of database fields it defines. Fields are specified by class attributes.
They are the equivalent to django Fields and therefore
the equivalent of columns in a traditional relational databases.

.. _fieldbase:

Field Base Class
==============================

.. autoclass:: stdnet.orm.Field
   :members:
   :member-order: bysource


.. _atomfield:

AtomField
==============================

.. autoclass:: stdnet.orm.AtomField
   :members:
   :member-order: bysource


IntegerField
==============================

.. autoclass:: stdnet.orm.IntegerField
   :members:
   :member-order: bysource


BooleanField
==============================

.. autoclass:: stdnet.orm.BooleanField
   :members:
   :member-order: bysource

   
AutoField
========================

.. autoclass:: stdnet.orm.AutoField
   :members:
   :member-order: bysource


FloatField
==============================

.. autoclass:: stdnet.orm.FloatField
   :members:
   :member-order: bysource   
   
.. _datefield:
   
DateField
==============================

.. autoclass:: stdnet.orm.DateField
   :members:
   :member-order: bysource
   

SymbolField
==============================

.. autoclass:: stdnet.orm.SymbolField
   :members:
   :member-order: bysource
   
   
CharField
==============================

.. autoclass:: stdnet.orm.CharField
   :members:
   :member-order: bysource

.. _foreignkey:
   
ForeignKey
==============================

.. autoclass:: stdnet.orm.ForeignKey
   :members:
   :member-order: bysource
   
