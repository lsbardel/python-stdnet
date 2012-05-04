.. _increase-performance:

.. module:: stdnet.odm

======================
Performance
======================

We dig deeper into our example by exploring additional features of
the API, including the manipulation of related models and transactions.


.. _model-transactions:

Transactions
========================

Under the hood, stdnet performs server updates and queries
via a :class:`Session`. You can write your application without
using a session directly, and in several cases this is good enough.
However, when dealing with lots of operations, you may be better off
using :class:`Transaction`. A transaction is started
with the :meth:`Session.begin` method and concluded with
the :meth:`Session.commit` method. A session for
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

This snippet demonstrates how to speed up the creation of several instances of
model ``Fund`` using a ``with`` statement::

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
to the server via the :meth:`commit` method.


.. _performance-loadonly:

Use load_only
================

One of the main advantages of using key-values databases as opposed to 
traditional relational databases, is the ability to add or remove
:class:`Field` without requiring database migration.
In addition, the :class:`JSONField` can be a factory
of fields for a given model (when used with the :attr:`JSONField.as_string`
set to ``False``).
For complex models, :class:`Field` can also be used as cache.

In these situations, your model may contain a lot of fields, some of which
could contain a lot of data (for example, text fields), or require
expensive processing to convert them to Python objects.
If you are using the results of a :class:`Query` in some situation
where you know you don't need those particular fields, you can tell stdnet
to load a subset from the database by using the :meth:`Query.load_only`
or :meth:`Query.dont_load` methods.

For example I need to load all my `EUR` Funds but I don't need to
see the description and documentation::

    qs = Fund.objects.filter(ccy = "EUR").load_only('name')

    

.. _performance-loadrelated:

Use load_related
====================


    