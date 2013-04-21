from stdnet.utils import test

from examples.models import Group, Person

class TestSignals(test.TestCase):
    models = (Group, Person)

    def addPerson(self, group, sigp, name=None):
        self.counter += 1
        self.assertEqual(name, 'pippo')
        self.assertEqual(group.id, 1)
        session = self.session()
        with session.begin() as t:
            u = Person(name=name, group=group)
            t.add(u)
        self.assertEqual(u.id, 1)

    def testPostCommit(self):
        self.counter = 0
        session = self.session()
        with session.begin() as t:
            g = Group(name='user').post_commit(self.addPerson, name='pippo')
            t.add(g)
            t.add(Group(name='admin'))
        self.assertEqual(g.id, 1)
        users = session.query(Person).filter(group__name='user')
        admins = session.query(Person).filter(group__name='admin')
        self.assertEqual(users.count(), 1)
        self.assertEqual(admins.count(), 0)
        self.assertEqual(self.counter, 1)