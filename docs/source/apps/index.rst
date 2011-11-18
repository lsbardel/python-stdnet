.. _contrib-index:

.. module:: stdnet.apps

================================
Applications
================================

The :mod:`stdnet.apps` module contains applications
which are based on :mod:`stdnet` but are not part of the
core library.

They don't have external dependencies but some require
a :ref:`non vanilla redis <stdnetredis>` implementation. They are here
mainly as use cases and in the future they may be removed
and placed into their own installable packages. 

.. toctree::
   :maxdepth: 2
   
   searchengine
   timeseries