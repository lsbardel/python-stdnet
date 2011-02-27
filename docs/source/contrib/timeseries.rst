.. _contrib-timeserie:


.. module:: stdnet.contrib.timeseries

============================
Timeseries Models
============================

This is an experimental module which requires ``redis-timeseries`` branch of redis_.
For more information check the :ref:`redis timeseries API <redis-timeseries>`.
To run the ``timeseries`` tests::

    python runtests.py timeseries -i ts



Timeseries base model
=========================

.. autoclass:: stdnet.contrib.timeseries.models.TimeSeriesBase
   :members:
   :member-order: bysource


Timeseries model
=========================

.. autoclass:: stdnet.contrib.timeseries.models.TimeSeries
   :members:
   :member-order: bysource
   

Timeseries Model Field
===============================

.. autoclass:: stdnet.contrib.timeseries.models.TimeSeriesField
   :members:
   :member-order: bysource


.. _redis: https://github.com/lsbardel/redis