.. _contrib-djangolink:

.. module:: stdnet.contrib.djangolink

============================
StdNet & Django Link
============================

This module is designed to make django_ models and ``stdnet`` models
work together by creating a one-to-one relationship between them.

To use this module you need to have django_ installed.


Examples
=========================



Using stdnet as cache
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The first example is simple, we create a link so that objects of 
specific django models are cached using stdnet::

	from django.db import models
	
	class Article(models.Model):
	    title = models.CharField(max_length = 200):
	    published = models.DateTime()
	    body = models.TextField()
	    
	class ArticleCache(orm.StdNet):
	    pass

And now we link them together using the :func:`stdnet.contrib.djangolink.link_models` function::

	stdnet.contrib.djangolink import link_models
	
	link_models(Article,ArticleCache)
	

API
===========

.. autofunction:: stdnet.contrib.djangolink.link_models



.. _django: http://www.djangoproject.com/