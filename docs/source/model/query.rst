.. _model-query:

.. module:: stdnet.orm.query

============================
Executing Queries
============================

Executing queries is very similar to executing queries in django_.
Once you've created your models, ``stdnet`` automatically gives you
a data-server abstraction API that lets you create, retrieve,
update and delete objects. 
This document explains how to use this API by using the following models
as example::

	from stdnet import orm
	
	class Fund(orm.StdModel):
	    name = orm.SymbolField(unique = True)
	    ccy = orm.SymbolField()
	    description = orm.CharField()
	    
	    def __str__(self):
        	return str(self.name)
	    
	class Instrument(orm.StdModel):
	    name = orm.SymbolField(unique = True)
	    ccy = orm.SymbolField()
	    type = orm.SymbolField()
	    prices = orm.HashField()
	    
	    def __str__(self):
        	return str(self.name)
	    
	class Position(orm.StdModel):
	    instrument = orm.ForeignKey(Instrument)
	    fund = orm.ForeignKey(Fund)
	    size = orm.FloatField()

These models are available in the :mod:`stdnet.tests.examples.models`, therefore you can import
them from there. Before playing with the API, :ref:`register the models <register-model>`.

Creating objects
======================
A model is mapped to a :class:`stdnet.HashTable` in a :class:`stdnet.BackendDataServer`
and an instance of that model represents a particular record
in the hash-table.

To create an object, instantiate it using keyword arguments to the
model class, then call ``save()`` to save it to the data-server.
Here's an example::

	>>> b = Fund(name='Markowitz', ccy='EUR')
	>>> b.save()
	Fund: Markowitz
	>>> b.id
	1
	>>> b.name
	'Markowitz'
	>>> b.ccy
	'EUR'
	>>> b.description
	''
	
The object ``b`` is a python representation of data stored in the server at ``id`` 1.
As discussed the :class:`stdnet.orm.StdModel` documentation, an instance of a model is
an mapped to an entry in a remote :class:`stdnet.HashTable` structure. If you want to see the actual struture you can procede as following::

	>>> meta = b._meta
	>>> t = meta.table()
	>>> t
	HashTable:stdnet.fund
	>>> t.id
	'stdnet.fund'
	
The hashtable ``id`` is the ``key`` used by the server to identify the structure.

	>>> t.size()
	1
	>>> list(t.keys())
	['1']
	>>> list(t.values())
	[['', 'EUR', 'Markowitz']]

	
Retrieving objects
==============================
To retrieve objects from your data sarver, you construct a :class:`stdnet.orm.query.QuerySet`
via a :class:`stdnet.orm.Manager` on your model class.

A QuerySet represents a collection of objects from your database.
It can have zero, one or many filters criteria that narrow down the collection
based on given parameters.

You get a QuerySet by using your model's Manager. Each model has at least one Manager,
and it's called objects by default. Access it directly via the model class::

	>>> Fund.objects
	<stdnet.orm.query.Manager object at ...>
	>>>

QuerySet Reference
==============================



.. autoclass:: stdnet.orm.query.QuerySet
   :members:
   :member-order: bysource
   
   
.. _django: http://www.djangoproject.com/