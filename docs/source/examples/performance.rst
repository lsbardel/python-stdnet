.. _increase-performance:

======================
Performance
======================


Use transactions
========================


Use load_related
====================




Use load_only
================

For example I need to load all my `EUR` Funds but I don't need to
see the description and documentation::

    qs = Fund.objects.filter(ccy = "EUR").load_only('name')
