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
contains two :class:`stdnet.orm.ForeignKey` fields.
In the context of relational databases a
`foreign key <http://en.wikipedia.org/wiki/Foreign_key>`_ is
a referential constraint between two tables.

For stdnet is exactly the same thing. The field store the ``id`` of a
related :class:`stdnet.orm.StdModel` instance.
Behind the scenes, this functionality is implemented by Python descriptors_.
This shouldn't really matter to you, but we point it out here for the curious.


.. _model-transactions:

Transactions
==========================

Under the hood, stdnet performs server updates and queries
via a :class:`stdnet.orm.Session`. You can write your application without
using a session directly, and in several cases this is good enough.
However, when dealing with lots of operations, you may be better off
using :ref:`transactions <transactions>`. A transaction is started
with the :meth:`stdnet.orm.Session.begin` method and concluded with
the :meth:`stdnet.orm.Session.commit` method. A session for
:ref:`registered models <register-model>` can be obtained from the model
manager. For example, using the ``Fund`` model in 
:ref:`tutorial 1 <tutorial-application>`::

    session = Fund.objects.session()
    session.begin() # start a new transaction
    session.add(Fund(name='Markowitz', ccy='EUR'))
    session.add(Fund(name='SterlingFund', ccy='GBP'))
    session.commit() # commit changes to the server


Transactions are pivotal for two reasons:

* They guarantee atomicity and therefore consistency of model instances when updating/deleting.
* They speed up updating, deleting and retrieval of several independent block
  of data at once.

For certain type of operations, the use of transactions becomes almost compulsory
as the speed up achived can be of 2 to 3 order of magnitude.

This is snippet demonstrates how to speed up the creation of several instances of
a model ``Fund`` using a ``with`` statement::

    with Fund.objects.transaction() as t:
        for kwargs in data:
            t.add(Fund(**kwargs))

Or for more than one model::


    with Fund.objects.transaction(Instrument) as t:
        for kwargs in data1:
            t.add(Fund(**kwargs))
        for kwargs in data2:
            t.add(Instrument(**kwargs))
        ...
        
        
As soon as the ``with`` statement finishes, the transaction commit changes
to the server via the :meth:`stdnet.orm.Session.commit` method.



.. _descriptors: http://users.rcn.com/python/download/Descriptor.htm