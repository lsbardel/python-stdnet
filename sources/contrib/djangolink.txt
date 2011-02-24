.. _contrib-djangolink:

.. module:: stdnet.contrib.djangolink

============================
StdNet & Django Link
============================

This is a battery included module avaiable at :mod:`stdnet.contrib.djangolink` useful for django_
developers needing to use a *smart cache* or adding extra in memory data to their relational
database models, or both.

To use this module you need to have django_ installed.


Stdnet as django model cache
===========================================

The first example is simple, we create a link so that objects of 
specific django models are cached using stdnet::

	from django.db import models
	from stdnet import orm
	
	class Article(models.Model):
	    title = models.CharField(max_length = 200):
	    published = models.DateTime()
	    body = models.TextField()
	    
	class ArticleCache(orm.StdNet):
	    pass

And now we link them together using the :func:`stdnet.contrib.djangolink.link_models` function::

	stdnet.contrib.djangolink import link_models
	
	link_models(Article,ArticleCache)
	

We can do more. Create ``stdnet`` filters by adding fields to our ``stdnet`` model::

	class ArticleCache(orm.StdNet):
	    title = orm.SymbolField()
	    
	link_models(Article,ArticleCache)
	

	


Adding Extra Data
=====================


	

API
===========


.. autofunction:: stdnet.contrib.djangolink.link_models



.. _django: http://www.djangoproject.com/