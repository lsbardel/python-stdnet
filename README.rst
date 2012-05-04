**A stand-alone Python 3 compatible data manager for Redis remote data structures.**

The data is owned by different, configurable back-end databases and it is accessed using a
light-weight Object Data Mapper (ODM). 
The `source code`__ and documentation__ are hosted at github while Downloads__ are available via PyPi.

:Documentation: http://lsbardel.github.com/python-stdnet/
:Dowloads: http://pypi.python.org/pypi/python-stdnet/
:Source: https://github.com/lsbardel/python-stdnet
:Issues: https://github.com/lsbardel/python-stdnet/issues
:Mailing List: https://groups.google.com/group/python-stdnet
:Keywords: server, database, cache, redis, odm


__ http://github.com/lsbardel/python-stdnet
__ http://lsbardel.github.com/python-stdnet/
__ http://pypi.python.org/pypi/python-stdnet/


Contents
~~~~~~~~~~~~~~~

.. contents::
    :local:
    

Requirements
=================
* Python 2.6 to Python 3.3. Single codebase.
* Optional Cython_ for faster redis protocol parser.
* You need access to a Redis_ server.


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
	
	
Documentation
============================
StdNet uses Sphinx_ for its documentation, and the latest is available at GitHub:

* http://lsbardel.github.com/python-stdnet/
	

Version Check
======================
To know which version you have installed::

	>>> import stdnet
	>>> stdnet.__version__
	'0.7c2'
	>>> stdnet.VERSION
	stdnet_version(major=0, minor=7, micro=0, releaselevel='rc', serial=2)


Backends
====================
Backend data-stores provide the backbone of the library,
while the Object Data Mapper the syntactic sugar.
Currently the list of back-ends is limited to

* Redis_.

There are plans to extend it to

* Local memory (planned). For testing purposes.
* Amazon DynamoDB_.
 
 
Object Data Mapper
================================
The ``stdnet.odm`` module is the ODM, it maps python object into database data.
It is design to be fast and safe to use::
 
	from stdnet import odm
 		
	class Base(odm.StdModel):
	    '''An abstract model. This won't have any data in the database.'''
	    # A unique symbol field, a symbol is an immutable string
	    name = odm.SymbolField(unique = True)
	    # Another symbol, symbol fields are by default indexes
	    ccy  = odm.SymbolField()
	    
	    def __str__(self):
	        return str(self.name)
	    
	    class Meta:
	        abstract = True
	
	
	class Instrument(Base):
	    itype = odm.SymbolField()
	
	    
	class Fund(Base):
		# A char field is a string and it is never an index
	    description = odm.CharField()
	
	
	class PositionDescriptor(odm.StdModel):
	    dt    = odm.DateField()
	    # A float field is not an index by default
	    size  = odm.FloatField()
	    price = odm.FloatField()
	    # A FK field which we explicitly set as non-index
	    position = odm.ForeignKey("Position", index = False)
	
	
	class Position(odm.StdModel):
	    instrument = odm.ForeignKey(Instrument, related_name = 'positions')
	    fund       = odm.ForeignKey(Fund)
	    history    = odm.ListField(model = PositionDescriptor)
	    
	    def __str__(self):
	        return '%s: %s @ %s' % (self.fund,self.instrument,self.dt)
	
	
	    
Register models with backend::

	odm.register(Instrument,'redis://localhost?db=1')
	odm.register(Fund,'redis://localhost?db=1')
	odm.register(PositionDescriptor,'redis://localhost?db=2')
	odm.register(Position,'redis://localhost?db=2')

And play with the API::

	>>> f = Fund(name="pluto,description="The super pluto fund",ccy="EUR").save()
	Fund: pluto


.. _runningtests:

Running Tests
======================
At the moment, only redis back-end is available and therefore to run tests you
need to install Redis_. If you are using linux, it can be achieved simply
by downloading, uncompressing and running ``make``, if you are using
windows and want to save yourself a headache you can download precompiled
binaries at servicestack__.

__ http://code.google.com/p/servicestack/wiki/RedisWindowsDownload

Requirements for running tests:

* unittest2_ for python 2.6 only.
* argparse_ for python 2.6, 3 and 3.1 only.
* nose_

Note, these requirements are only needed if you are planning to run tests.
To run tests open a shell and launch Redis. On another shell,
from the package directory, type::

    python runtests.py
    
Tests are run against a local redis server on port 6379 and database 7 by default.
To change the server and database where to run tests pass the ``--server`` option as follow::

    python runtests.py --server redis://myserver.com:6450/?db=12

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
* redis-py_ for the Redis Python client initial implementation which has been subsequently modified.
* hiredis-py_ for some parts of the C parser.
* SQLAlchemy_ and Django_ for ideas and API design.
* Armin Ronacher and Ask Solem for the celery sphinx theme used for the documentation.


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
.. _Redis: http://redis.io/
.. _hiredis-py: https://github.com/pietern/hiredis-py
.. _Django: http://www.djangoproject.com/
.. _SQLAlchemy: http://www.sqlalchemy.org/
.. _redis-py: http://github.com/andymccurdy/redis-py
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