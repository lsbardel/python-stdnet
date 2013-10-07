from stdnet.utils import test
from stdnet import odm

from examples.models import Group, Person


class TestSignals(test.TestWrite):
    models = (Group, Person)

    def setUp(self):
        models = self.mapper
        models.post_commit.bind(self.addPerson, sender=Group)

    def addPerson(self, signal, sender, instances=None, session=None,
                  **kwargs):
        models = session.router
        self.assertEqual(models, self.mapper)
        session = models.session()
        with session.begin() as t:
            for instance in instances:
                self.counter += 1
                if instance.name == 'user':
                    t.add(models.person(name='luca', group=instance))
        return t.on_result

    def testPostCommit(self):
        self.counter = 0
        session = self.session()
        with session.begin() as t:
            g = Group(name='user')
            t.add(g)
            t.add(Group(name='admin'))
        yield t.on_result
        self.assertEqualId(g, 1)
        users = session.query(Person).filter(group__name='user')
        admins = session.query(Person).filter(group__name='admin')
        yield self.async.assertEqual(users.count(), 1)
        yield self.async.assertEqual(admins.count(), 0)
        self.assertEqual(self.counter, 2)
