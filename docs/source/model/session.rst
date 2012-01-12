.. _model-session:

.. module:: stdnet.orm

============================
Sessions
============================

The :class:`Session` is designed along the lines of sqlalchemy_. It establishes
all conversations with the database and represents a “holding zone” for all the
objects which you’ve loaded or associated with it during its lifespan.

It also provides the entrypoint to acquire a :class:`Query` object, which sends
queries to the database using the :attr:`Session.backend`, the session
database connection.


Getting a session
=====================

:class:`Session` is a regular Python class which can be directly instantiated
by passing the backend connection string or an instance of
a class:`stdnet.BackendDataServer`.::

    from stdnet import orm
    
    session = orm.Session('redis://localhost:8060?db=3')
    


Session API
===================

.. autoclass:: Session
   :members:
   :member-order: bysource
   
   
Managers API
=====================

Manager
~~~~~~~~~~~~~~~~~~
.. autoclass:: Manager
   :members:
   :member-order: bysource
   
   
.. module:: stdnet.orm.related

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
   
Many2ManyRelatedManager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: Many2ManyRelatedManager
   :members:
   :member-order: bysource
   
.. _sqlalchemy: http://www.sqlalchemy.org/docs/orm/session.html