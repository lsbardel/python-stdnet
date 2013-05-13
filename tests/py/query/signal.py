from stdnet.utils import test
from stdnet import odm

from examples.models import Group, Person


class TestSignals(test.TestCase):
    models = (Group, Person)

    def addPerson(self, group, sigp, instances=None, **kwargs):
        with self.session().begin() as t:
            for instance in instances:
                self.counter += 1
                if instance.name == 'user':
                    t.add(self.mapper.user(name=name, group=instance))
        return t.on_result

    def testPostCommit(self):
        self.counter = 0
        session = self.session()
        odm.post_commit.connect(self.addPerson, sender=Group)
        with session.begin() as t:
            g = Group(name='user')
            t.add(g)
            t.add(Group(name='admin'))
        yield t.on_result
        self.assertEqualId(g.id, 1)
        users = session.query(Person).filter(group__name='user')
        admins = session.query(Person).filter(group__name='admin')
        yield self.async.assertEqual(users.count(), 1)
        yield self.async.assertEqual(admins.count(), 0)
        self.assertEqual(self.counter, 2)