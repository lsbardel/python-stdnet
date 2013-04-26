.. _tutorial-related:

.. module:: stdnet.odm

============================
Related Models
============================

There are two :class:`Fields` which represents relationaships between
:class:`StdModel`. 


.. _one-to-many:

One-to-many relationships
================================

The :ref:`Position model <tutorial-application>` in our example,
contains two :class:`ForeignKey`
fields which represent relationships between the *Position* model and
the *Instrument* model and between the *Position* model and the *Fund* model.

In the context of relational databases a
`foreign key <http://en.wikipedia.org/wiki/Foreign_key>`_ is
a referential constraint between two tables.
For stdnet is exactly the same thing. The field store the ``id`` of a
related :class:`StdModel` instance.

**Key Properties**

* Behind the scenes, stdnet appends ``_id`` to the field name to create its
  field name in the back-end data-server. In other words the
  :ref:`Position model <tutorial-application>` will store in the backend
  an object with the following entries::
  
        {'instrument_id': ...,
         'fund_id': ...,
         'size': ...
         'dt': ...}
       
* The attribute of a :class:`ForeignKey` can be used to access the related
  object. Using the :ref:`router we created during registration <tutorial-registration>`
  we get a position instance::
  
        p = router.position.get(id=1)
        p.instrument    # an instance of Instrument
  
  The second statement is equivalent to::
  
        router.instrument.query().get(id=p.instrument_id)
  
  .. note::      
  
    The loading of the related object is done, **once only**, the first time
    the attribute is accessed. This means, the first time you access a related
    field on a model instance, there will be a roundtrip to the backend server.
  
  Behind the scenes, this functionality is implemented by Python
  descriptors_. This shouldn't really matter to you, but we point it out here
  for the curious.
  
* Depending on your application, sometimes it makes a lot of sense to use the
  :ref:`load_related query method <performance-loadrelated>` to boost
  performance when accessing many related fields.
  
* When the object referenced by a :class:`ForeignKey` is deleted, stdnet also
  deletes the object containing the :class:`ForeignKey` unless the
  :class:`Field.required` attribute of the :class:`ForeignKey` field is set
  to ``False``.



.. _many-to-many:

Many-to-many relationships
==================================

The :class:`ManyToManyField` can be used to create relationships between
multiple elements of two models. It requires a positional argument, the class
to which the model is related.

Behind the scenes, stdnet creates an intermediary model to represent
the many-to-many relationship. We refer to this as the ``through model``.

Let's consider the following example::

    class Group(odm.StdModel):
        name = odm.SymbolField(unique=True)

    class User(odm.StdModel):
        name = odm.SymbolField(unique=True)
        groups = odm.ManyToManyField(Group, related_name='users')

Both the ``User`` class and instances of if have the ``groups`` attribute which
is an instance of A many-to-may :class:`stdnet.odm.related.One2ManyRelatedManager`.
Accessing the manager via the model class or an instance has different outcomes.


.. _through-model:

The through model
~~~~~~~~~~~~~~~~~~~~~~~

Custom through model
~~~~~~~~~~~~~~~~~~~~~~

In most cases, the standard through model implemented by stdnet is
all you need. However, sometimes you may need to associate data with the
relationship between two models.

For these situations, stdnet allows you to specify the model that will be used
to govern the many-to-many relationship and pass it to the
:class:`ManyToManyField` constructor via the ``through`` argument.
Consider this simple example::

    from stdnet import odm

    class Element(odm.StdModel):
        name = odm.SymbolField()
    
    class CompositeElement(odm.StdModel):
        weight = odm.FloatField()
    
    class Composite(odm.StdModel):
        name = odm.SymbolField()
        elements = odm.ManyToManyField(Element, through=CompositeElement,
                                       related_name='composites')


.. _descriptors: http://users.rcn.com/python/download/Descriptor.htm