.. _tutorial-search:

.. module:: stdnet.odm

=========================================
Search for text in your models
=========================================

Full text search in most key-value stores is not provided out of the box as
it is in traditional relational databases.

One may argue that no-sql databases don't need to provide such feature since
search engines such as Solr_, ElasticSearch_ or Sphinx_ can be used to
provide a full text search solution.

Stdnet provides an :ref:`interface for searching <search>` so that such
engines can be used by third party applications.
 

.. module:: stdnet.apps.searchengine

Redis based solution
========================

Stdnet also provides a redis-based implementation so that you can have your models
stored and indexed in the same redis instance.
This is a great solution during development and for medium sized applications.
Installing the search engine is explained in
:ref:`redis search <apps-searchengine>` documention. It is as easy as

* :ref:`Registering <register-model>` the :class:`WordItem` model which stores
  the indices for your models
* Create the search engine singletone::

    from stdnet.apps.searchengine import SearchEngine
    
    se = SearchEngine()

* Register models you want to search to the search engine signletone::

    se.register(MyModel)

searching model instances for text can be achieved using the
:class:`Query.search` method::

    MyModel.objects.query().search('bla foo...') 


.. _solr: http://lucene.apache.org/solr/
.. _ElasticSearch: http://www.elasticsearch.org/
.. _Sphinx: http://sphinxsearch.com/

