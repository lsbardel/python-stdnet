.. _model-field-structure:

.. module:: stdnet.orm

============================
Data-structure Fields
============================

These fields are remote data-structures such as list, sets and hash tables.
They can be bound to models so that
many-to-many objects relationship can be established. All the data-structure
fields derives from MultiField.

.. autoclass:: stdnet.orm.MultiField
   :members:
   :member-order: bysource



ListField
==============================

.. autoclass:: stdnet.orm.ListField
   :members:
   :member-order: bysource
   

SetField
==============================

.. autoclass:: stdnet.orm.SetField
   :members:
   :member-order: bysource
   
   
HashField
==============================

.. autoclass:: stdnet.orm.HashField
   :members:
   :member-order: bysource


ManyToManyField
==============================

.. autoclass:: stdnet.orm.ManyToManyField
   :members:
   :member-order: bysource

   