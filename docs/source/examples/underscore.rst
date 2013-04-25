.. _tutorial-underscore:

.. module:: stdnet.odm

=======================================
Double Underscore Notation
=======================================

Stdnet makes extensive use of the ``__`` **double-underscore notation** in
several parts of the API.

* A :class:`Query` with :ref:`range <range-lookups>` and
  :ref:`text <text-lookups>` lookups::
  
    qs = MyModel.objects.filter(size__gt=40, description__contains='technology')
      
* An :class:`Query` on a :class:`Field` of a related model. For example, in the
  :ref:`Position model <tutorial-application>` one can :ref:`filter <tutorial-filter>`,
  :ref:`exclude <tutorial-exclude>` or :ref:`sort <explicit-sorting>`, with respect
  the instrument ``ccy`` :class:`Field` in this way::
  
    qs = Position.objects.filter(instrument__ccy='EUR')
    qs = Position.objects.exclude(instrument__ccy='EUR')
    qs = Position.objects.query().sort_by('instrument__ccy')

* In conjunction with :ref:`load_only <performance-loadonly>` query method when
  you need to load only a subset of a related model fields::
  
        qs = Position.objects.query().load_only('size', 'instrument__ccy')
        
* In the :meth:`StdModel.get_attr_value` method, for example::

    p.get_attr_value('instrument')
    # same as
    p.instrument
    
  and::
  
    p.get_attr_value('instrument__ccy')
    # same as
    p.instrument.ccy
  
    
    