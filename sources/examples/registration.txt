
.. _tutorial-registration:


.. module:: stdnet.odm

================================
Registration
================================

Registration consists in associating a :class:`StdModel` to a :class:`Manager`
via a :class:`Router`. In this way one can have a group of models associated
with their managers pointing at their, possibly different, back-end servers.
Registration is straightforward and as shown in the
:ref:`tutorial application <tutorial>` it is achieved by::

    from stdnet import odm
    
    models = odm.Router('redis://127.0.0.1:6379?db=7&password=bla')
    
    models.register(Instrument)
    models.register(Fund)
    models.register(Position)

The :ref:`connection string <connection-string>` passed as first argument when
initialising a :class:`Router`, is the default back-end of that :class:`Router`.
It is possible to register models to a different back-end by passing a connection
string to the :meth:`Router.register` method::

    models.register(MyModel, 'redis://127.0.0.1:6379?db=8&password=bla')
    

Accessing managers
=======================
Given a ``models`` :class:`Router` there are two ways one can access a
model :class:`Manager` to perform database queries.

.. _router-dict:

Dictionary interface
~~~~~~~~~~~~~~~~~~~~~~~~
The most straightforward and intuitive way, for accessing managers, is to use
the :class:`Router` as a dictionary of :class:`Manager`::


    # Crate a Query for Instrument
    query = models[Instrument].query()
    #
    # Create a new Instrument and save it to the backend server
    inst = models[Instrument].new(...)

.. _router-dotted:

Dotted notation
~~~~~~~~~~~~~~~~~~~~~~~~
An alternative to the dictionary interface is the dotted notation, where
a manager can be accessed as an attribute of the :class:`Router`, the attribute
name is given by the :class:`StdModel` metaclass name. This is the
:attr:`ModelMeta.name` attribute of the :attr:`Model._meta` object.
It is, by default, the class name of the model in lower case::

    query = models.instrument.query()
    #
    inst = models.instrument.new(...)
    
This interface is less verbose than the :ref:`dictionary notation <router-dict>`
and, importantly, it reduces to zero the imports one has to write on python
modules using your application, in other words it makes your application
less dependent on the actual implementation of :class:`StdModel`.


Multiple backends
=========================

The :class:`Router` allows to use your models in several different back-ends
without changing the way you query your data. In addition it allows to
specify different back-ends for ``write`` operations and for ``read`` only
operations.

To specify a different back-end for read operations one registers a model in
the following way::

    models.register(Position, 'redis://127.0.0.1:6379?db=8&password=bla',
                    'redis://127.0.0.1:6380?db=1')


.. _custom-manager:

Custom Managers
============================

When a :class:`Router` registers a :class:`StdModel`, it creates a new
instance of a :class:`Manager` and add it to the dictionary of managers.
It is possible to supply a custom manager class by specifying the
``manager_class`` attribute on the :class:`StdModel`::

    class CustomManager(odm.Manager):
    
        def special_query(self, ...):
            return self.query().filter(...)
            
            
    class MyModel(odm.StdModel):
        ...
        
        manager_class = CustomManager 

Model Iterator
============================

.. autofunction:: model_iterator
