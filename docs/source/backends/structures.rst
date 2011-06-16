.. _structures-backend:

.. module:: stdnet.backends.structures

==============================
Remote Structure
==============================


Data structures are fundamental constructs around which you build your application.
They are used in almost all computer programs, therefore becoming fluent in what the standard
data-structures can do for you is essential to get full value out of them.
The `standard template library`_ in C++ implements a wide array of structures,
python has several of them too. ``stdnet`` implements **five** remote structures,
all derived from :class:`stdnet.Structure`.


Structure Base Class
======================

.. autoclass:: stdnet.Structure
   :members:
   :member-order: bysource


List
==============================

.. autoclass:: stdnet.List
   :members:
   :member-order: bysource


Set
==============================

.. autoclass:: stdnet.Set
   :members:
   :member-order: bysource


.. _orderedset-structure:

OrderedSet
==============================

.. autoclass:: stdnet.OrderedSet
   :members:
   :member-order: bysource


.. _hash-structure:   
   
HashTable
==============================

.. autoclass:: stdnet.HashTable
   :members:
   :member-order: bysource
   
   
TS
==============================

.. autoclass:: stdnet.TS
   :members:
   :member-order: bysource
   
.. _standard template library: http://www.sgi.com/tech/stl/
