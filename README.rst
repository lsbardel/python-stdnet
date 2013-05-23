**Object data mapper and advanced query manager for non relational databases.**

The data is owned by different, configurable back-end databases and it is
accessed using a light-weight Object Data Mapper (ODM). The ODM presents a
method of associating user-defined Python classes with database **collections**,
and instances of those classes with **items** in their corresponding collections.
Collections and items are different for different backend databases but
are treated in the same way in the python language domain.

:Master CI: |master-build|_ 
:Dev CI: |dev-build|_ 
:Documentation: http://pythonhosted.org/python-stdnet/
:Dowloads: http://pypi.python.org/pypi/python-stdnet/
:Source: https://github.com/lsbardel/python-stdnet
:Mailing List: https://groups.google.com/group/python-stdnet
:Keywords: server, database, cache, redis, mongo, odm


.. |master-build| image:: https://secure.travis-ci.org/lsbardel/python-stdnet.png?branch=master
.. _master-build: http://travis-ci.org/lsbardel/python-stdnet
.. |dev-build| image:: https://secure.travis-ci.org/lsbardel/python-stdnet.png?branch=dev
.. _dev-build: http://travis-ci.org/lsbardel/python-stdnet

Contents
~~~~~~~~~~~~~~~

.. contents::
    :local:
    

Features
=================
* Models with scalar and multi-value fields.
* Rich query API including unions, intersections, exclusions, ranges and more.
* Minimal server round-trips via backend scripting (lua for redis).
* Full text search.
* Signals handling to allow decoupled applications to get notified on changes.
* Synchronous and asynchronous database connection.
* Multi-variate numeric timeseries application.
* Asynchronous Publish/Subscribe application.
* 90% Test coverage.
* Fully documented.

Requirements
=================
* Python 2.6, 2.7, 3.2, 3.3 and pypy_. Single code-base.
* redis-py_ for redis backend.
* Optional pymongo_ for the mongo backend.
* Optional pulsar_ when using the asynchronous connections or the test suite.
* You need access to a Redis_ server version 2.6 or above and/or a Mongo_ server.


Philosophy
===============
Key-valued pairs databases, also know as key-value stores, have many differences
from traditional relational databases,
most important being they do not use ``SQL`` as their query language,
storage does not require a fixed table schemas and usually they do not support
complex queries.

Stdnet aims to accommodate a flexible schema and join type operations via
a lightweight object data mapper.
Importantly, it is designed with large data sets in mind. You pull data
you need, nothing more, nothing less.
Bandwidth and server round-trips can be reduced to the bare minimum
so that your application is fast and memory efficient.


Installing 
================================
To install, download, uncompress and type::

	python setup.py install

otherwise use ``easy_install``::

	easy_install python-stdnet
	
or ``pip``::

	pip install python-stdnet
	

Version Check
======================
To know which version you have installed::

	>>> import stdnet
	>>> stdnet.__version__
	'0.8.0'
	>>> stdnet.VERSION
	stdnet_version(major=0, minor=8, micro=0, releaselevel='final', serial=1)


Backends
====================
Backend data-stores are the backbone of the library.
Currently the list is limited to

* Redis_ 2.6 or above.
* Mongodb_ (alpha).
 
 
Object Data Mapper
================================
The ``stdnet.odm`` module is the ODM, it maps python objects into database data
and vice-versa. It is design to be fast and safe to use::
 
	from stdnet import odm
 		
	class Base(odm.StdModel):
	    '''An abstract model. This won't have any data in the database.'''
	    name = odm.SymbolField(unique = True)
	    ccy  = odm.SymbolField()
	    
	    def __unicode__(self):
	        return self.name
	    
	    class Meta:
	        abstract = True
	
	
	class Instrument(Base):
	    itype = odm.SymbolField()
	
	    
	class Fund(Base):
	    description = odm.CharField()
	
	
	class PositionDescriptor(odm.StdModel):
	    dt    = odm.DateField()
	    size  = odm.FloatField()
	    price = odm.FloatField()
	    position = odm.ForeignKey("Position", index=False)
	
	
	class Position(odm.StdModel):
	    instrument = odm.ForeignKey(Instrument, related_name='positions')
	    fund       = odm.ForeignKey(Fund)
	    history    = odm.ListField(model=PositionDescriptor)
	    
	    def __unicode__(self):
	        return '%s: %s @ %s' % (self.fund,self.instrument,self.dt)
	
	
	    
Register models with backend::

    models = orm.Router('redis://localhost?db=1')
    models.register(Instrument)
    models.register(Fund)
    models.register(PositionDescriptor,'redis://localhost?db=2')
    models.register(Position,'redis://localhost?db=2')

And play with the API::

	>>> f = models.fund.new(name="pluto, description="The pluto fund", ccy="EUR")
	>>> f
	Fund: pluto


.. _runningtests:

Running Tests
======================
At the moment, only redis back-end is available and therefore to run tests you
need to install Redis_. If you are using linux, it can be achieved simply
by downloading, uncompressing and running ``make``, if you are using
windows you can find sources from MSOpenTech_.

Requirements for running tests:

* ``python-stdnet`` project directory.
* pulsar_.

To run tests open a shell and launch Redis. On another shell,
from within the ``python-stdnet`` package directory, type::

    python runtests.py
    
Tests are run against a local redis server on port ``6379`` and database 7 by default.
To change the server and database where to run tests pass the ``--server``
option as follow::

    python runtests.py --server redis://myserver.com:6450?db=12&password=bla

For more information type::

    python runtests.py -h 

To access coverage of tests you need to install the coverage_ package and run the tests using::

    coverage run runtests.py
    
and to check out the coverage report::

    coverage html
    
    
.. _kudos:

Kudos
=============
* Redis_ simply because this library uses its awesome features.
* SQLAlchemy_ and Django_ for ideas and API design.


.. _contributing:

Contributing
=================
Development of stdnet happens at Github: http://github.com/lsbardel/python-stdnet

We very much welcome your contribution of course. To do so, simply follow these guidelines:

1. Fork python-stdnet on github
2. Create a topic branch ``git checkout -b my_branch``
3. Push to your branch ``git push origin my_branch``
4. Create an issue at https://github.com/lsbardel/python-stdnet/issues with a link to your patch


.. _license:

License
=============
This software is licensed under the New BSD_ License. See the LICENSE
file in the top distribution directory for the full license text.

.. _Cython: http://cython.org/
.. _redis-py: https://github.com/andymccurdy/redis-py
.. _Redis: http://redis.io/
.. _Mongo: http://www.mongodb.org/
.. _hiredis-py: https://github.com/pietern/hiredis-py
.. _pymongo: http://pypi.python.org/pypi/pymongo/
.. _Django: http://www.djangoproject.com/
.. _SQLAlchemy: http://www.sqlalchemy.org/
.. _ORM: http://en.wikipedia.org/wiki/Object-relational_mapping
.. _CouchDB: http://couchdb.apache.org/
.. _couchdb-python: http://code.google.com/p/couchdb-python/
.. _Memcached: http://memcached.org/
.. _BSD: http://www.opensource.org/licenses/bsd-license.php
.. _Sphinx: http://sphinx.pocoo.org/
.. _coverage: http://nedbatchelder.com/code/coverage/
.. _argparse: http://pypi.python.org/pypi/argparse
.. _unittest2: http://pypi.python.org/pypi/unittest2
.. _nose: http://readthedocs.org/docs/nose/en/latest
.. _DynamoDB: http://aws.amazon.com/dynamodb/
.. _pulsar: http://pypi.python.org/pypi/pulsar
.. _mock: http://pypi.python.org/pypi/mock
.. _pypy: http://pypy.org/
.. _Mongodb: http://www.mongodb.org/
.. _MSOpenTech: https://github.com/MSOpenTech/redis