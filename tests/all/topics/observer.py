from random import randint
from time import time

from stdnet.utils import test

from examples.observer import Observer, Observable, update_observers


class ObserverData(test.DataGenerator):
    sizes = {'tiny': (2, 5),   # observable, observers
             'small': (5, 20),
             'normal': (10, 80),
             'big': (50, 500),
             'huge': (100, 10000)}

    def generate(self):
        self.observables, self.observers = self.size


class ObserverTest(test.TestWrite):
    multipledb = 'redis'
    models = (Observer, Observable)
    data_cls = ObserverData

    def setUp(self):
        models = self.mapper
        #
        # Register post commit callback
        models.post_commit.bind(update_observers, sender=Observable)
        #
        # Create the observables and the observers
        session = models.session()
        with session.begin() as t:
            for i in range(self.data.observables):
                t.add(models.observable())
            for i in range(self.data.observers):
                t.add(models.observer())
        yield t.on_result
        observables = t.saved[models.observable]
        observers = t.saved[models.observer]
        N = len(observables)
        self.created = {}
        #
        with session.begin() as t:
            for observer in observers:
                # pick a random number of observables to track
                self.created[observer.id] = created = set()
                # The first observervable is observed by all observers
                created.add(observables[0])
                observer.underlyings.add(observables[0])
                for i in range(randint(1, N-1)):
                    o = observables[randint(0, N-1)]
                    created.add(o)
                    observer.underlyings.add(o)
        yield t.on_result
        # We should have added several observer_observalbles through instances
        self.assertEqual(len(t.saved), 1)
        self.observables = observables
        self.observers = observers

    def test_meta(self):
        models = self.mapper
        zset = models.observer.updates
        self.assertEqual(zset.penalty, 5)
        self.assertTrue(zset.session is not None)
        self.assertTrue(zset.field)
        self.assertEqual(zset.model, Observer)
        #
        backend = zset.backend_structure()
        self.assertEqual(backend.instance, zset)

    def test_created(self):
        models = self.mapper
        observers = yield models.observer.all()
        self.assertEqual(len(observers), self.data.observers)
        for o in observers:
            created = self.created[o.id]
            observables = yield o.underlyings.all()
            self.assertEqual(created, set(observables))

    def test_simple_save(self):
        '''Save the first observable and check for updates.'''
        models = self.mapper
        obs = self.observables[0]
        now = time()
        yield self.async.assertEqual(models.observer.updates.size(), 0)
        yield obs.save()
        # The first observable is connected with all observers
        updates = yield models.observer.updates.size()
        self.assertEqual(updates, self.data.observers)
        #
        data = yield models.observer.updates.irange(0, -1)
        self.assertEqual(len(data), self.data.observers)
        # Check the score
        for n, sv in enumerate(data, 1):
            score, id = sv
            self.assertTrue(score > now)

    def test_save_all(self):
        models = self.mapper
        with models.session().begin() as t:
            for o in self.observables:
                t.add(o)
        yield t.on_result
        now = time()
        # The first observable is connected with all observers
        # therefore we have all observers to be updated
        updates = yield models.observer.updates.size()
        self.assertEqual(updates, self.data.observers)
        #
        data = yield models.observer.updates.irange()
        self.assertEqual(len(data), self.data.observers)
        # Check the score
        #
        penalty = models.observer.updates.penalty
        prev = None
        #
        observers = dict(((o.id, o) for o in self.observers))
        for score, id in data:
            observer = observers[id]
            N = yield observer.underlyings.query().count()
            if prev:
                self.assertTrue(N <= prev)
            prev = N
