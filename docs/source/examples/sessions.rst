.. _model-session:

.. module:: stdnet.odm

============================
Sessions
============================

The :class:`Session` is designed along the lines of SQLAlchemy_. It establishes
all conversations with the database and represents a *holding zone* for all the
objects which you have loaded or associated with it during its lifespan.

It also provides the entry point to acquire a :class:`Query` object, which sends
queries to the database using the :attr:`Session.backend`, the session
database connection.


Getting a session
=====================

:class:`Session` is a regular Python class which can be directly instantiated
by passing the backend connection string or an instance of
a class:`stdnet.BackendDataServer`.::

    from stdnet import odm
    
    session = odm.Session('redis://localhost:8060?db=3')
    
    
Query a model
====================

Once a session is obtained, one can create a query on a model by simply invoking
the :meth:`Session.query` method::

    query = session.query(MyModel)
    
    
.. _SQLAlchemy: http://www.sqlalchemy.org/  