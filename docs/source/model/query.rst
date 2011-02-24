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
	    
	    def __unicode__(self):
        	return self.name
	    
	class Instrument(orm.StdModel):
	    name = orm.SymbolField(unique = True)
	    ccy = orm.SymbolField()
	    type = orm.SymbolField()
	    prices = orm.HashField()
	    
	    def __unicode__(self):
        	return self.name
	    
	class Position(orm.StdModel):
	    instrument = orm.ForeignKey(Instrument)
	    fund = orm.ForeignKey(Fund)
	    size = orm.FloatField()
	    
	    def __unicode__(self):
        	return self.instrument

These models are available in the :mod:`tests.examples.models` module in the distribution directory.
Before playing with the API you need to :ref:`register the models <register-model>`::

	import orm

	orm.register(Fund, 'redis://my.host.name:6379/?db=1')
	orm.register(Instrument, 'redis://my.host.name:6379/?db=1')
	orm.register(Position, 'redis://my.host.name:6379/?db=2')
	

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
	
The object ``b`` is a ``python`` representation of data stored in the server at ``id`` 1.
As discussed the :class:`stdnet.orm.StdModel` documentation, an instance of a model is
mapped to an entry in a remote :class:`stdnet.HashTable` structure.
If you want to see the actual struture you can procede as following::

	>>> meta = b._meta
	>>> t = meta.table()
	>>> t
	HashTable:stdnet.fund
	>>> t.id
	'stdnet.fund'
	
The hash-table ``id`` is the ``key`` used by the server to identify the structure.

	>>> t.size()
	1
	>>> list(t.keys())
	['1']
	>>> list(t.values())
	[['', 'EUR', 'Markowitz']]

	
Retrieving objects
==============================
To retrieve objects from your data server, you construct a :class:`stdnet.orm.query.QuerySet`
via a :class:`stdnet.orm.query.Manager` on your model class.

A QuerySet represents a collection of objects from your database.
It can have zero, one or many filters criteria that narrow down the collection
based on given parameters.

You get a QuerySet by using your model's Manager. Each model has at least one Manager,
and it's called objects by default. Access it directly via the model class::

	>>> Fund.objects
	<stdnet.orm.query.Manager object at ...>
	>>>

Retrieving all objects
~~~~~~~~~~~~~~~~~~~~~~~~~~~
The simplest way to retrieve objects from a table is to get all of them. To do this, use the :meth:`stdnet.orm.query.Manager.all`
method on a Manager:

	>>> funds = Fund.objects.all()
	>>> funds
	QuerySet
	>>> funds._seq
	>>> list(funds)
	[Fund: Markowitz]
	>>> funds._seq
	[Fund: Markowitz]

QuerySet are lazy, they are evaluated only when you iterate over them.
The results are then stored in the ``_seq`` attribute.

Retrieving filtered objects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Usually, you'll need to select only a subset of the complete set of objects.
To create such a subset, you refine the initial QuerySet, adding filter conditions.
Lets create few other objects in the same line as above and try::

	>>> eur_funds = Fund.objects.filter(ccy = 'EUR')
	>>> eur_funds
	QuerySet.filter({'ccy': 'EUR'})
	>>> eur_funds.count()
	1
	>>> list(eur_funds)
	[Fund: Markowitz]

The ``count`` method counts the object in the query without physically retrieving them.


Retrieving from a list (equivalent to a select where in SQL)::

	Fund.objects.filter(ccy__in = ('EUR','USD'))
	
	
Concatenating queries::

	Instrument.objects.filter(ccy__in = ('EUR','USD')).filter(types__in = ('equity',bond'))
	
You can also exclude fields from lookups::

	Instrument.objects.exclude(type = 'future')
	
and so forth. The API is very similar to django_, but it is for an unstructured-in memory database.

QuerySet API Reference
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
   
.. _django: http://www.djangoproject.com/