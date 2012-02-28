.. _increase-performance:

======================
Performance
======================


Use transactions
========================


.. _performance-loadrelated:

Use load_related
====================




.. _performance-loadonly:

Use load_only
================

One of the main advantages of using key-values databases as opposed to 
traditional relational databases, is the ability to add or remove
:class:`stdnet.orm.Field` without requiring database migration.
In addition, the :class:`stdnet.orm.JSONField` can be a factory
of fields for a given model (when used with the :attr:`stdnet.orm.JSONField.as_string`
set to ``False``).
For complex models, :class:`stdnet.orm.Field` can also be used as cache.

In these situations, your model may contain a lot of fields, some of which
could contain a lot of data (for example, text fields), or require
expensive processing to convert them to Python objects.
If you are using the results of a :class:`stdnet.orm.Query` in some situation
where you know you don't need those particular fields, you can tell stdnet
to load a subset from the database.

For example I need to load all my `EUR` Funds but I don't need to
see the description and documentation::

    qs = Fund.objects.filter(ccy = "EUR").load_only('name')

    