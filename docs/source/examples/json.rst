.. _field-generator:

.. module:: stdnet.odm

=======================
Field Generator
=======================

This tutorial is about the use of :class:`JSONField` with parameter
:class:`JSONField.as_string` set to ``False``.
Lets start with a model for a general data feed::

    class Feed(odm.StdModel):
        name = odm.SymbolField()
        data = odm.JSONField(as_string=False)

lets create an instance::

    feed = Feed(name='goog').save()
    feed.data = {'price': {'bid': 750, 'offer':751},
                 'volume': 2762355371,
                 'mkt_cap': '255B',
                 'pe': 23}
    feed.save()
    
When loading the instance one can access all the fields in the following way::

    feed.data['price']['bid']
    feed.data['price']['offer']
    
or equivalently::

    feed.data__price__bid
    feed.data__price__offer

    
         
         