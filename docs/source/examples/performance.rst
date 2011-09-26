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

For example I need to load all my `EUR` Funds but I don't need to
see the description and documentation::

    qs = Fund.objects.filter(ccy = "EUR").load_only('name')
