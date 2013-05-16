.. _tutorial-serialise:

.. module:: stdnet.odm

=======================
Export & Load data
=======================

Stdnet comes with utilities for exporting and loading models from files. These
:ref:`serialization utilities <serialize-models>` are useful for backing up
your models, porting your data to other databases or creating test databases.

There are two serializers included in the standard distribution: **json** and **csv**.

Exporting Data
====================

To export data is quite strightforward, you create a :class:`Query` and
pass it to a :meth:`Serializer.dump` method. Using the
:ref:`models router <tutorial-models-router>` in the first tutorial::

    >>> from stdnet import odm
    >>> json = odm.get_serializer('json')
    >>> qs = models.instrument.query()
    >>> json.dump(qs)
    
So fat the ``json`` serializer has not written anything to file. Therefore
we can add additional queries::

    >>> qs = models.fund.query()
    >>> json.dump(qs)

To write to file we use the :meth:`Serializer.write` methods::

    >>> with open('data.json','w') as stream:
    >>>     json.write(stream)
     

Loading Data
====================

To load data from a file or a stream we starts from a :class:`Router`
which contains all the models we need::

    >>> json = odm.get_serializer('json')
    >>> with open('data.json','r') as f:
    >>>     data = f.read()
    >>> json.load(models, data)


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

To be able to use the ``MySerializer`` it needs to be registered via
the :func:`register_serializer`::

    odm.register_serializer('custom', MySerializer)