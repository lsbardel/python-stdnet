.. _model-query:

.. module:: stdnet.orm.query

============================
Executing Queries
============================

Executing queries is very similar to executing queries in django_.
Once you've created your models, ``stdnet`` automatically gives you
a data-server abstraction API that lets you create, retrive,
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
	
	


QuerySet Reference
==============================

.. autoclass:: stdnet.orm.query.QuerySet
   :members:
   :member-order: bysource
   
   
.. _django: http://www.djangoproject.com/