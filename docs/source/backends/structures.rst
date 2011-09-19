.. _structures-backend:

.. module:: stdnet.backends.structures

==============================
Remote Data Structures
==============================


Data structures are fundamental constructs around which you build your application.
They are used in almost all computer programs, therefore becoming fluent in what the standard
data-structures can do for you is essential to get full value out of them.
The `standard template library`_ in C++ implements a wide array of structures,
python has several of them too. ``stdnet`` implements **five** remote structures:
list, set, ordered set, hash and timeseries.

The structures are bind to a remote dataserver and they derive from
from :class:`stdnet.Structure` base class.


.. module:: stdnet

Creating Structures
==========================

The struct object
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. py:attribute:: struct

    Creating the five structures available in stdnet is accomplished by using struct
    object in the following way::
    
        from stdnet import struct
    
        l = struct.list()
        s = struct.set()
        o = struct.zset()
        h = struct.hash()
        t = struct.ts()
    
   

.. module:: stdnet.backends.structures
Low level API
======================

Structure Base Class
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.Structure
   :members:
   :member-order: bysource


List
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.List
   :members:
   :member-order: bysource


Set
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.Set
   :members:
   :member-order: bysource


.. _orderedset-structure:

OrderedSet
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.OrderedSet
   :members:
   :member-order: bysource


.. _hash-structure:   
   
HashTable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.HashTable
   :members:
   :member-order: bysource
   
   
TS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: stdnet.TS
   :members:
   :member-order: bysource
   
.. _standard template library: http://www.sgi.com/tech/stl/
