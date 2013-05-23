.. _tutorial-search:

.. module:: stdnet.odm

=========================================
Full text search
=========================================

Full text search in most key-value stores is not provided out of the box as
it is in traditional relational databases.
One may argue that no-sql databases don't need to provide such feature since
search engines such as Solr_, ElasticSearch_ or Sphinx_ can be used to
provide a full text search solution.

Stdnet provides the :class:`stdnet.odm.SearchEngine` interface
for implementing full text search of stdnet models. The interface
can be customized with third party applications.
 

Redis based solution
========================

.. automodule:: stdnet.apps.searchengine

.. _solr: http://lucene.apache.org/solr/
.. _ElasticSearch: http://www.elasticsearch.org/
.. _Sphinx: http://sphinxsearch.com/

