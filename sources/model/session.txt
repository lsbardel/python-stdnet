.. _model-session:

.. module:: stdnet.odm

============================
Sessions
============================

The :class:`Session` is designed along the lines of sqlalchemy_. It establishes
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
    


Session API
===================

Session
~~~~~~~~~~~~~~~

.. autoclass:: Session
   :members:
   :member-order: bysource
   
Session Model
~~~~~~~~~~~~~~~

.. autoclass:: SessionModel
   :members:
   :member-order: bysource
   

Transaction
~~~~~~~~~~~~~~~

.. autoclass:: Transaction
   :members:
   :member-order: bysource
   
Managers API
=====================

Manager
~~~~~~~~~~~~~~~~~~
.. autoclass:: Manager
   :members:
   :member-order: bysource
   
   
.. module:: stdnet.odm.related

RelatedManager
~~~~~~~~~~~~~~~~~~

.. autoclass:: RelatedManager
   :members:
   :member-order: bysource
   
One2ManyRelatedManager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: One2ManyRelatedManager
   :members:
   :member-order: bysource

   
.. _sqlalchemy: http://www.sqlalchemy.org/docs/orm/session.html