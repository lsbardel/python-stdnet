.. _model-index:

.. module:: stdnet.odm

============================
Object Data Mapper
============================

The *object-data mapper* is the core of the library. It defines an API for mapping
data in the backend key-value store to objects in Python.
It'is name is closely related to
`object relational Mapping <http://en.wikipedia.org/wiki/Object-relational_mapping>`_ (ORM),
a programming technique for converting data between incompatible
type systems in traditional `relational databases <http://en.wikipedia.org/wiki/Relational_database>`_
and object-oriented programming languages.

There are two well known ORMs for Python: SQLAlchemy_ and Django_ models.
Both of them are fully feature open-source libraries with an incredible community
of users and developers. 

Stdnet is aN ``ODM``, an object data mapper for non-conventional databases or nosql_ as
they are known. It is also a lightweight module, which deal only with data mapping,
advanced queries and nothing else.


**Contents**

.. toctree::
   :maxdepth: 2
   
   models
   fields
   session
   search

.. _SQLAlchemy: http://www.sqlalchemy.org/   
.. _Django: http://docs.djangoproject.com/en/dev/ref/models/instances/
.. _nosql: http://nosql-database.org/