.. _tutorial2:

===========================================
Tutorial 2
===========================================

We dig deeper into our example by exploring additional features of
the API, including the manipulation of related models and transactions.

.. _one-to-many:

One-to-many relationships
================================

The *Position* model defined in :ref:`tutorial 1 <tutorial-application>`
contains a :class:`stdnet.orm.ForeignKey` field.
In the context of relational databases a
`foreign key <http://en.wikipedia.org/wiki/Foreign_key>`_ is
a referential constraint between two tables.



Behind the scenes, this functionality is implemented by Python descriptors_.
This shouldn't really matter to you, but we point it out here for the curious.


.. _model-transactions:

Transactions
==========================

Stdnet performs server updates via transactions and allows the use of
transaction for speeding up commands by pipelining them into a single
request for the back-end server.
By default transaction are only used when updating/deleting a single instance
of a model or a single data structure.

So for example when you call the :meth:`stdnet.orm.StdModel.save`
or the :meth:`stdnet.Structure.save` methods without any parameter, the change
will be committed immediately.


Usage
~~~~~~~~~~~~~~~~~~~~~~

Transactions are pivotal for two reasons:

* They guarantee atomicity and therefore consistency of model instances when updating/deleting.
* They speed up updating, deleting and retrieval of several independent block
  of data at once.

For certain type of operations, the use of transactions becomes almost compulsory
as the speed up achived can be of 2 to 3 order of magnitude.

This is snippet demonstrates how to speed up the creation of several instances of
a model ``MyModel``::

    with MyModel.transaction() as t:
        for kwargs in data:
            MyModel(**kwargs).save(t)

Or for more than one model::

    import stdnet
    
    with stdnet.transaction(MyModel1,MyModel2,...,ModelN) as t:
        for kwargs in data1:
            MyModel1(**kwargs).save(t)
        for kwargs in data2:
            MyModel2(**kwargs).save(t)
        ...
        
The high level API function :func:`stdnet.transaction` creates an instance of
:class:`stdnet.Transaction` which aggregate all queries and updates without
communicating with the server.

As soon as the ``with`` statement finishes, the transaction commit changes
to the server via the :meth:`stdnet.Transaction.commit` method.

        





.. _descriptors: http://users.rcn.com/python/download/Descriptor.htm