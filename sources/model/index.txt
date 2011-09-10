.. _model-index:

============================
Object Relational Mapper
============================

`Object Relational Mapping <http://en.wikipedia.org/wiki/Object-relational_mapping>`_ (ORM) in
computer software is a programming technique for converting data between incompatible
type systems in databases and object-oriented programming languages.

There are two well known ORMs for Python: SQLAlchemy_ and Django_ models.
Both of them are fully feature open-source libraries with an incredible community
of users and developers.
At the moment, they are best suited for traditional
`relational databases <http://en.wikipedia.org/wiki/Relational_database>`_ back-ends. 

On the other hand, ``stdnet`` is geared towards non-conventional databases or nosql_ as
they are known. It is also a lightweight module, which deal only with ORM and nothing else.
Well, that is not strictly true, there is a ``contrib`` module, from which ``stdnet`` does not depend,
where several applications will be added.


**Contents**

.. toctree::
   :maxdepth: 2
   
   models
   fields

.. _SQLAlchemy: http://www.sqlalchemy.org/   
.. _Django: http://docs.djangoproject.com/en/dev/ref/models/instances/
.. _nosql: http://nosql-database.org/