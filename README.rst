**A Python 3 compatible object relational mapper for Redis remote data structures.**

The data is owned by different, configurable back-end databases and it is accessed using a
light-weight Object Relational Mapper (ORM_) inspired by Django_ and SQLAlchemy_. 
The `source code`__ and documentation__ are hosted at github while Downloads__ are available via PyPi.

--

:Documentation: http://lsbardel.github.com/python-stdnet/
:Dowloads: http://pypi.python.org/pypi/python-stdnet/
:Source: https://github.com/lsbardel/python-stdnet
:Issues: https://github.com/lsbardel/python-stdnet/issues
:Mailing List: https://groups.google.com/group/python-stdnet
:Keywords: server, database, cache, redis, orm


__ http://github.com/lsbardel/python-stdnet
__ http://lsbardel.github.com/python-stdnet/
__ http://pypi.python.org/pypi/python-stdnet/


Requirements
=================
* You need access to a Redis_ server.
* Python 2.6 or above, including Python 3 series.

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
	'0.6.1'


.. _runningtests:

Running Tests
======================
At the moment, only redis back-end is available and therefore to run tests you need to install Redis_.
If you are using linux, it can be achieved simply by downloading, uncompressing and running ``make``, if you are using
windows and want to save yourself a headache you can download precompiled binaries at servicestack__.

__ http://code.google.com/p/servicestack/wiki/RedisWindowsDownload

If you are running python 2.6, 3 and 3.1 you need to install the argparse_ package,
which is a standard in python 2.7 and python >= 3.2.
Once done that, open a shell and launch Redis. On another shell, from the package directory,
type::

	python runtests.py
	
Tests are run against a local redis server on port 6379 and database 7 by default.
To change the server and database where to run tests pass the ``-s`` option as follow::

    python runtests.py -s redis://myserver.com:6450/?db=12

For more information type::

    python runtests.py -h 

To access coverage of tests you need to install the coverage_ package and run the tests using::

	coverage run runtests.py
	
and to check out the coverage report::

	coverage report -m


Backends
====================
Backend data-stores provide the backbone of the library,
while the Object Relational Mapper the syntactic sugar.
Currently the list of back-ends is limited to

* Redis_.
* Local memory (planned). For testing purposes.

**Only** Redis_ **is operational.**
 
 
Object Relational Mapper
================================
The module ``stdnet.orm`` is the ORM, it maps python object into database data. It is design to be fast and
safe to use::
 
	from stdnet import orm
 		
	class Base(orm.StdModel):
	    '''An abstract model. This won't have any data in the database.'''
	    # A unique symbol field, a symbol is an immutable string
	    name = orm.SymbolField(unique = True)
	    # Another symbol, symbol fields are by default indexes
	    ccy  = orm.SymbolField()
	    
	    def __str__(self):
	        return str(self.name)
	    
	    class Meta:
	        abstract = True
	
	
	class Instrument(Base):
	    itype = orm.SymbolField()
	
	    
	class Fund(Base):
		# A char field is a string and it is never an index
	    description = orm.CharField()
	
	
	class PositionDescriptor(orm.StdModel):
	    dt    = orm.DateField()
	    # A float field is not an index by default
	    size  = orm.FloatField()
	    price = orm.FloatField()
	    # A FK field which we explicitly set as non-index
	    position = orm.ForeignKey("Position", index = False)
	
	
	class Position(orm.StdModel):
	    instrument = orm.ForeignKey(Instrument, related_name = 'positions')
	    fund       = orm.ForeignKey(Fund)
	    history    = orm.ListField(model = PositionDescriptor)
	    
	    def __str__(self):
	        return '%s: %s @ %s' % (self.fund,self.instrument,self.dt)
	
	
	    
Register models with backend::

	orm.register(Instrument,'redis://localhost/?db=1')
	orm.register(Fund,'redis://localhost/?db=1')
	orm.register(PositionDescriptor,'redis://localhost/?db=2')
	orm.register(Position,'redis://localhost/?db=2')

And play with the API::

	>>> f = Fund(name="pluto,description="The super pluto fund",ccy="EUR").save()
	Fund: pluto

Kudos
=============
* Redis_ simply because this library uses its awesome features.
* redis-py_ for the Redis Python client initial implementation which has been subsequently modified.
* Django_ for some ideas and the ``dispatch`` module.
* Armin Ronacher and Ask Solem for the celery sphinx theme used for the documentation.


Contributing
=================
Development of StdNet happens at Github: http://github.com/lsbardel/python-stdnet

You are highly encouraged to participate in the development. Here how to do it:

1. Fork python-stdnet on github
2. Create a topic branch (git checkout -b my_branch)
3. Push to your branch (git push origin my_branch)
4. Create an issue at https://github.com/lsbardel/python-stdnet/issues with a link to your patch


Licence
=============
This software is licensed under the New BSD_ License. See the LICENSE
file in the top distribution directory for the full license text.

.. _Redis: http://code.google.com/p/redis/
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