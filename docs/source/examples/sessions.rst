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


Obtaining a session
=====================

:class:`Session` is a regular Python class which can be directly instantiated
by passing the backend connection string or an instance of
a :class:`stdnet.BackendDataServer`::

    from stdnet import odm
    
    session = odm.Session('redis://localhost:8060?db=3')
    
    
Alternatively, if a model ``MyModel`` has been :ref:`registered <register-model>`,
with a :class:`Router` instance ``router``, one can obtain a session,
from the model manager::
 
    session = router[myModel].session()
    
or using dotted notation::

    session = router.mymodel.session()
     
    
Query a model
====================

Once a session is obtained, one can create a query on a model by simply invoking
the :meth:`Session.query` method::

    query = session.query(MyModel)
    
    
.. _transactional-state:

Transactional State
=========================

A :class:`Session` is said to be in a **transactional state** when its
:class:`Session.transaction` attribute is not ``None``. A transactional state is
obtained via the :meth:`Session.begin` method::

    transaction = session.begin()
    
The returned transaction instance is the same as the value stored at the
:class:`Session.transaction` attribute. Note that if we try to obtain a new transaction
from a session already in a transactional state an :class:`InvalidTransaction`
exception will occur.
 
    
.. _SQLAlchemy: http://www.sqlalchemy.org/  