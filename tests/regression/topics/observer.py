from random import randint
from time import time

from stdnet import odm, test

from examples.observer import Observer, Observable


class ObserverTest(test.TestCase):
    models = (Observer,Observable)
    
    def setUp(self):
        self.register()
        for i in range(5):
            Observable().save()
        obs = Observable.objects.query().sort_by('id').all()
        for i in range(20):
            observer = Observer().save()
            N = randint(1,5)
            for i in range(N):
                observer.underlyings.add(obs[i])
            self.assertEqual(observer.underlyings.query().count(),N)
    
    def testMeta(self):
        s = Observer.updates
        self.assertEqual(s.penalty,5)
            
    def testSimpleSave(self):
        obs = Observable.objects.query().sort_by('id').all()
        obs1 = obs[0]
        now = time()
        self.assertEqual(Observer.updates.size(), 0)
        obs1.save()
        # The first observable is connected with all observers
        self.assertEqual(Observer.updates.size(), 20)
        data = list(Observer.updates.irange(0,-1))
        self.assertEqual(len(data), 20)
        for n,sv in enumerate(data,1):
            score, id = sv
            self.assertTrue(score > now)
            
    def testSave(self):
        obs = Observable.objects.query().sort_by('id').all()
        for o in obs:
            o.save()
        now = time()
        # The first observable is connected with all observers
        self.assertEqual(Observer.updates.size(), 20)
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
        