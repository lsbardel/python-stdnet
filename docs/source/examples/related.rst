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
  object::
  
        p = Position.objects.get(id=1)
        p.instrument    # an instance of Instrument
  
  The second statement is equivalent to::
  
        Instrument.objects.query().get(id=p.instrument_id)
        
  The loading of the related object is done, once only, the first time the attribute
  is accessed. Behind the scenes, this functionality is implemented by Python
  descriptors_. This shouldn't really matter to you, but we point it out here
  for the curious.
  
* When the object referenced by a :class:`ForeignKey` is deleted, stdnet also
  deletes the object containing the :class:`ForeignKey` unless the
  :class:`Field.required` attribute of the :class:`ForeignKey` field is set
  to ``False``.



.. _many-to-many:

Many-to-many relationships
==================================

The :class:`ManyToManyField` is used to create relationships multiple elements
of two models.


.. _descriptors: http://users.rcn.com/python/download/Descriptor.htm