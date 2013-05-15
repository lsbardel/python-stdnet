from random import randint
from time import time

from stdnet.utils import test

from examples.observer import Observer, Observable


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
    
    def tearDown(self):
        return self.clear_all()
        
    def test_meta(self):
        models = self.mapper
        s = models.observer.updates
        self.assertEqual(s.penalty, 5)
        self.assertTrue(s.session is not None)
        self.assertTrue(s.is_field)
        
    def test_created(self):
        observers = yield self.models.observer.query().all()
        self.assertEqual(len(observers), self.data.observers)
        for o in observers:
            created = self.created[o.id]
            observables = yield o.underlyings.all()
            self.assertEqual(created, set(observables))
            
    def test_simple_save(self):
        models = self.mapper
        obs = yield models.observable.query().get(id=1)
        now = time()
        yield self.async.assertEqual(models.observer.updates.size(), 0)
        yield obs.save()
        # The first observable is connected with all observers
        updates = yield models.observer.updates.size()
        self.assertEqual(updates, self.data.observers)
        data = list(Observer.updates.irange(0,-1))
        self.assertEqual(len(data), 20)
        for n,sv in enumerate(data,1):
            score, id = sv
            self.assertTrue(score > now)
            
    def test_save(self):
        models = self.mapper
        obs = yield models.observable.query().sort_by('id').all()
        with models.session().begin() as t:
            for o in obs:
                t.add(o)
        yield t.on_result
        now = time()
        # The first observable is connected with all observers
        self.assertEqual(models.observer.updates.size(), 20)
        data = list(Observer.updates.irange(0,-1))
        self.assertEqual(len(data), 20)
        penalty = Observer.updates.penalty
        prev = None
        for score, id in data:
            observer = Observer.objects.get(id = id)
            N = observer.underlyings.query().count()
            if prev:
                self.assertTrue(N <= prev)
            prev = N
        