
.. _sorting:

=======================
Sorting and Ordering
=======================
Stdnet can sort instances of a model in three different ways:

* :ref:`Explicit sorting <explicit-sorting>` using the
  :attr:`stdnet.orm.Query.sort_by` method.
* :ref:`Implicit sorting <implicit-sorting>` via the
  :attr:`stdnet.orm.Metaclass.ordering` attribute of the model metaclass.
* :ref:`Incremental sorting <incremental-sorting>`, a variant of the
  implicit sorting for models which require to keep track how many
  times instances with the same id are created.


.. _explicit-sorting:

Explicit Sorting
=======================

Sorting is usually achieved by using the :meth:`stdnet.orm.query.QuerySet.sort_by`
method with a field name as parameter. Lets consider the following model::

    class SportActivity(orm.StdNet):
        person = orm.SymbolField()
        activity = orm.SymbolField()
        dt = orm.DateTimeField()
        

To obtained a sorted query on dates for a given person::

    SportActivity.objects.filter(person='pippo').sort_by('-dt')

The negative sign in front of ``dt`` indicates descending order.


.. _implicit-sorting:

Implicit Sorting
===================

Implicit sorting is achieved by setting the ``ordering`` attribute in the model Meta class.
Let's consider the following Log model example::

    class Log(orm.StdModel):
        '''A database log entry'''
        timestamp = orm.DateTimeField(default=datetime.now)
        level = orm.SymbolField()
        msg = orm.CharField()
        source = orm.CharField()
        host = orm.CharField()
        user = orm.SymbolField(required=False)
        client = orm.CharField()
    
        class Meta:
            ordering = '-timestamp'

It makes lots of sense to have the log entries always sorted in a descending
order with respect to the ``timestamp`` field.
This solution always returns querysets in this order, without the need to
call ``sort_by`` method.

.. note:: Implicit sorting is a much faster solution than explicit sorting,
          since there is no sorting step involved (which is a ``N log(N)``
          time complexity algorithm). Instead, the order is maintained by using
          sorted sets as indices rather than sets.

   
.. _incremental-sorting:

Incremental Sorting
========================
