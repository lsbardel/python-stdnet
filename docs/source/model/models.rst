.. _model-model:

.. module:: stdnet.orm.models

============================
StdNet Models
============================

The StdNet Object Relational Mapper presents a method of
associating user-defined Python classes, referred as **models**,
with :class:`stdnet.HashTable` structures in
a :class:`stdnet.BackendDataServer`.
These python classes
are referred as **models** and are subclasses of
:class:`stdnet.orm.StdModel`.

Each instance of model represents a key-value
in the corresponding :class:`stdnet.HashTable`.

.. autoclass:: stdnet.orm.StdModel
   :members:
   :member-order: bysource


.. _creating-models:

Creating Models
======================

Defining a stdnet models is simple, you derive a Python class from ``StdModel``::

	from stdnet import orm
	
	class Author(orm.StModel):
	    name = orm.SymbolField()
	
	class Book(orm.StdModel):
	    title  = orm.CharField()
	    author = orm.ForeignKey(Author, related_name = 'books')
	    
	    
.. _register-model:
	    
Register A Model
=====================

Once a model is defined, in order to use it in an application it needs to be registered with
a back-end database.

.. autofunction:: stdnet.orm.register

 
.. _database-metaclass:

Data Server Metaclass
==========================

.. autoclass:: stdnet.orm.base.Metaclass
   :members:
   :member-order: bysource