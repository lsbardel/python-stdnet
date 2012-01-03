.. _model-session:

.. module:: stdnet.orm

============================
Sessions
============================

The :class:`Session` is designed along the lines of sqlalchemy_. It establishes
all conversations with the database and represents a “holding zone” for all the
objects which you’ve loaded or associated with it during its lifespan.


Session
===================

.. autoclass:: Session
   :members:
   :member-order: bysource
   
   
Managers
=====================

Manager
~~~~~~~~~~~~~~~~~~
.. autoclass:: Manager
   :members:
   :member-order: bysource
   
RelatedManager
~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.query.RelatedManager
   :members:
   :member-order: bysource
   
M2MRelatedManager
~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.orm.query.M2MRelatedManager
   :members:
   :member-order: bysource
   
.. _sqlalchemy: http://www.sqlalchemy.org/docs/orm/session.html