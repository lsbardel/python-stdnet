.. _tutorial-asynchronous:


========================
Asynchronous usage
========================

Stdnet has been designed so that it can operate in fully asynchronous mode
if the backend connection class is asynchronous.
The :ref:`redis backend <redis-server>` is shipped with an asychronous client
written using pulsar_.

When using stdnet in asynchronous mode, a user writes generator method
which pulsar_ treats as asynchronous coroutines. In this way adding instances
of Found model in :ref:`our tutorial application <tutorial-application>`
becomes::

    fund = yield Fund(name='Markowitz', ccy='EUR').save()
    
or using a :ref:`session <model-session>`::

    with Fund.objects.session().begin() as t:
        t.add(Fund(name='Markowitz', ccy='EUR'))
    yield t.on_result
    


.. _pulsar: http://quantmind.github.com/pulsar/
