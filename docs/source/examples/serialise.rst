.. _tutorial-serialise:

.. module:: stdnet.odm

=======================
Export & Load data
=======================

Stdnet comes with utilities for exporting and loading models from files. These
:ref:`serialization utilities <serialize-models>` are useful for backing up
your models, porting your data to other databases or creating test databases.

There are two serializers included in the standard distribution: **json** and **csv**.


Creating a Serializer
==========================
To create a new serializer, one starts by subclassing the :class:`Serializer`
and implement the :meth:`Serializer.dump` and :meth:`Serializer.load` and
:meth:`Serializer.write` methods::

    from stdnet import dm
    
    class MySerializer(odm.Serializer):
        
        def dump(self, qs):
            ...
            
        def write(self, stream=None):
            ...
            
        def load(self, stream, model=None):
            ...