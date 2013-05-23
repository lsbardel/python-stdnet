.. _model-session:

.. module:: stdnet.odm

============================
Sessions
============================

A :class:`Session` is a lightweight component which establishes all
conversations with backend databases. It is the middleware
between :class:`Model` and :class:`Router` on one side and the
:class:`stdnet.BackendDataServer` on the other side.


Obtaining a session
=====================

:class:`Session` is a regular Python class which is obtained from
a :class:`Router` via the :meth:`Router.session` method. We continue to use the
:ref:`models router <tutorial-models-router>` 
created for our :ref:`tutorial application <tutorial-application>`::
    
    session = models.session()
    session2 = models.session()
     

Query a model
====================

Once a session is obtained, one can create a query on a model by simply invoking
the :meth:`Session.query` method::

    query = session.query(models.fund)
    
A less verbose way of obtaining a query is to use the :meth:`Manager.query`
method directly::

    query = models.fund.query()
    
    
    
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
 
  