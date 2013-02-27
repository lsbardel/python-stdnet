.. _tutorial-underscore:


=======================================
Double Underscore Notation
=======================================

Stdnet makes extensive use of the ``__`` **double-underscore notation** in
several parts of the API.

* An :class:`stdnet.Query` with :ref:`range lookups <range-lookups>`::

        qs = MyModel.objects.filter(size__gt=40)
      
* An :class:`stdnet.Query` on a field of a related model. For example, in the
  :ref:`Position model <tutorial-application>` one can :ref:`filter <tutorial-filter>`,
  :ref:`filter <tutorial-exclude>` or :ref:`sort <explicit-sorting>`, with respect
  the instrument ``ccy`` field in this way::
  
        qs = Position.objects.filter(instrument__ccy='EUR')
        qs = Position.objects.exclude(instrument__ccy='EUR')
        qs = Position.objects.query().sort_by('instrument__ccy')

* In conjunction with :ref:`load_only <performance-loadonly>` query method when
  you need to load only a subset of a related model fields::
  
        qs = Position.objects.query().load_only('size', 'instrument__ccy')