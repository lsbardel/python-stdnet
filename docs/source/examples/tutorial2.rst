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


Behind the scenes, this functionality is implemented by Python descriptors_.
This shouldn't really matter to you, but we point it out here for the curious.


.. _model-transactions:

Transactions
==========================

Since version 0.5.6, stdnet performs server updates via transactions.
Transactions are pivotal for two reasons:

* They guarantee atomicity and therefore consistency of model instances when updating/deleting.
* To speed up updating/deleting of several instances at once.

For certain type of operations, the use of transactions becomes almost compulsory
as the speed up achived can be of 2 to 3 order of magnitude.
A tipical usage to speed up the creation of several instances of
a model ``MyModel``::

    with MyModel.transaction() as t:
        for kwargs in data:
            MyModel(**kwargs).save(t)

Or for more than one model::

    from stdnet import orm
    
    with orm.transaction(MyModel1,MyModel2,...,ModelN) as t:
        for kwargs in data1:
            MyModel1(**kwargs).save(t)
        for kwargs in data2:
            MyModel2(**kwargs).save(t)
        ...
        
The :func:`stdnet.orm.transaction` creates an instance of
:class:`stdnet.Transaction` which aggregate all queries and updates without
communicating with the server.

        





.. _descriptors: http://users.rcn.com/python/download/Descriptor.htm