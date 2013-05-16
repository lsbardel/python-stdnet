.. _local-models:

.. module:: stdnet.odm

======================
Local Models
======================

Stdnet provides a tool for creating and manipulating :class:`Model` which
are not backed by a :class:`BackendDataServer` but requires an
interface similar to :class:`StdModel` classes.


Creating a model
=======================

The primary tool for creating local models is the :func:`create_model`
API function::

    create_model('RedisDb', 'db')
