
:Documentation: http://packages.python.org/python-stdnet/
:Dowloads: http://pypi.python.org/pypi/python-stdnet/
:Source: http://code.google.com/p/python-stdnet/
:Keywords: server, database, cache, redis, orm

--

**An object relational mapper fro remote data structures.**

The data is owned by different, configurable back-end databases and it is accessed using a
light-weight Object Relational Mapper (ORM_) inspired by Django_ and SQLAlchemy_. 
The source code is hosted at `google code`__ while
Documentation__ and Downloads__ are available via PyPi.

__ http://code.google.com/p/python-stdnet/
__ http://packages.python.org/python-stdnet/
__ http://pypi.python.org/pypi/python-stdnet/


Requirements
=================
* You need access to a Redis_ server.
* redis-py_ version 2.0 or higher.


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
To know whech version you have installed::

	>>> import stdnet
	>>> stdnet.__version__
	'0.3.2'


Running Tests
======================
At the moment, only redis back-end is available, so to run tests you need to install redis.
Once done that, launch redis and type::

	>>> import stdnet
	>>> stdnet.runtests()
	
otherwise from the package directory::

	python runtests.py

	
Default settings
=========================
Running tests with the above commands assumes your Redis_ server
is running on the same machine.

StdNet comes with two default settings.

	>>> from stdnet.conf import settings
	>>> settings.__dict__
	{'DEFAULT_BACKEND': 'redis://127.0.0.1:6379/?db=7', 'DEFAULT_KEYPREFIX': 'stdnet'}

If your redis server runs on a different machine,
you need to setup a	script file along these lines::
	
	if __name__ == '__main__':
	    from stdnet.conf import settings
	    settings.DEFAULT_BACKEND = 'redis://your.server.url:6379/?db=10'
	    import stdnet
	    stdnet.runtests()


Backends
====================
Backend data-stores provide the backbone of the library,
while the Object Relational Mapper the syntactic sugar.
Currently the list of back-ends is limited to

* Redis_. Requires redis-py_.
* Local memory (planned). For testing purposes.
* CouchDB_ (planned). Requires couchdb-python_.

**Only** Redis_ **is operational.**
 
Object Relational Mapper
================================
The module ``stdnet.orm`` is a lightweight ORM::
 
	from stdnet import orm
 		
	class Base(orm.StdModel):
	    name = orm.SymbolField(unique = True)
	    ccy  = orm.SymbolField()
	    
	    def __str__(self):
	        return str(self.name)
	    
	    class Meta:
	        abstract = True
	
	class Instrument(Base):
	    type = orm.SymbolField()
	    
	class Fund(Base):
	    description = orm.CharField()
	
	class PositionDescriptor(orm.StdModel):
	    dt    = orm.DateField()
	    size  = orm.FloatField()
	    price = orm.FloatField()
	    position = orm.ForeignKey("Position")
	
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