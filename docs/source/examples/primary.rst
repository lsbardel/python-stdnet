.. _tutorial-primary-unique:

.. module:: stdnet.odm

=======================================
Primary Keys and Unique Fields
=======================================

Each :class:`StdModel` must have a primary key. The model primary key :class:`Field`
can be obtained via the :meth:`StdModel.pk` class method. A model can have one and only one
primary key which is specified by passing ``primary_key=True`` during
model definition::

    class MyModel(odm.StdModel):
        id = odm.SymbolField(primary_key=True)
        ...

A primary key field has the :attr:`Field.primary_key` attribute ``True``.

.. _tutorial-compositeid:

Composite ID
=========================

The :class:`CompositeIdField` enforces a group of fields in a model to be
unique (together). Let`s consider the following model::

    class Vote(StdModel):
        full_name = odm.SymbolField()
        address = odm.SymbolField()
        result = odm.SymbolField()
        
Now I want to make ``full_name`` and ``address`` unique together, so that
given a name and address I can uniquely identify a vote.
This is achieved by introducing a :class:`CompositeIdField`::

    class Vote(StdModel):
        id = odm.CompositeIdField('full_name', 'address')
        full_name = odm.SymbolField()
        address = odm.SymbolField()
        result = odm.SymbolField()

.. note::

    The :class:`CompositeIdField` is used, behind the scenes,
    by the :ref:`through model <through-model>` in the :class:`ManyToManyField`.
    
    
.. _tutorial-unique:

Unique Fields
=========================

A :class:`Field` can be enforced to be unique by passing the ``unique=True``
during model definition. The following models specifies two fields to be unique
across the whole model instances::

    class MyModel(odm.StdModel):
        full_name = odm.SymbolField()
        username = odm.SymbolField(unique=True)
        email = odm.SymbolField(unique=True)