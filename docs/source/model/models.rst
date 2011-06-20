.. _model-model:

.. module:: stdnet.orm.models

============================
Model and Query API
============================

Model
==================

The StdNet Object Relational Mapper presents a method of
associating user-defined Python classes, referred as **models**,
with :class:`stdnet.HashTable` structures in
a :class:`stdnet.BackendDataServer`.
These python classes
are referred as **models** and are subclasses of
:class:`stdnet.orm.StdModel`.


StdModel Class
~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.StdModel
   :members:
   :member-order: bysource


.. _creating-models:

Creating Models
~~~~~~~~~~~~~~~~~~~~~~~

Defining a stdnet models is simple, you derive a Python class from ``StdModel``::

	from stdnet import orm
	
	class Author(orm.StModel):
	    name = orm.SymbolField()
	
	class Book(orm.StdModel):
	    title  = orm.CharField()
	    author = orm.ForeignKey(Author, related_name = 'books')
	    
 
.. _database-metaclass:

Data Server Metaclass
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.base.Metaclass
   :members:
   :member-order: bysource


        
.. _register-model:
        
Registration
======================

Once a model is defined, in order to use it in an application it needs to be
registered with a back-end database.

Register
~~~~~~~~~~~~~~~~

.. autofunction:: stdnet.orm.register


Register application models
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autofunction:: stdnet.orm.register_application_models   

   
Query
==============================


QuerySet
~~~~~~~~~~~~~~~
Though you usually won't create one manually, you'll go through a :class:`stdnet.orm.query.Manager`,
here's the formal declaration of a QuerySet.

.. autoclass:: stdnet.orm.query.QuerySet
   :members:
   :member-order: bysource
   

Manager
~~~~~~~~~~~~~~~
.. autoclass:: stdnet.orm.query.Manager
   :members:
   :member-order: bysource