from datetime import datetime
from random import randint, choice

from stdnet import odm
from stdnet.utils import test, zip, populate

from examples.models import User, Post


class TwitterData(test.DataGenerator):
    sizes = {'tiny': (10, 5),
             'small': (30, 10),
             'normal': (100, 30),
             'big': (1000, 100),
             'huge': (100000, 1000)}

    def generate(self):
        size, _ = self.size
        self.usernames = self.populate('string', size=size, min_len=5,
                                       max_len=20)
        self.passwords = self.populate('string', size=size, min_len=8,
                                       max_len=20)

    def followers(self):
        _, max_size = self.size
        min_size = max_size // 2
        return randint(min_size, max_size)


class TestTwitter(test.TestWrite):
    models = (User, Post)
    data_cls = TwitterData

    def setUp(self):
        with self.mapper.session().begin() as t:
            for username, password in zip(self.data.usernames,
                                          self.data.passwords):
                t.add(User(username=username, password=password))
        return t.on_result

    def testMeta(self):
        following = User.following
        followers = User.followers
        self.assertEqual(following.formodel,User)
        self.assertEqual(following.relmodel,User)
        self.assertEqual(followers.formodel,User)
        self.assertEqual(followers.relmodel,User)
        self.assertEqual(following.model, followers.model)
        self.assertEqual(len(following.model._meta.dfields),3)
        self.assertEqual(following.name_relmodel, 'user')
        self.assertEqual(following.name_formodel, 'user2')
        self.assertEqual(followers.name_relmodel, 'user2')
        self.assertEqual(followers.name_formodel, 'user')

    def testRelated(self):
        models = self.mapper
        users = models.user.query()
        user1 = users[0]
        user2 = users[1]
        user3 = users[2]
        r = user1.following.add(user3)
        self.assertEqual(r.user, user1)
        self.assertEqual(r.user2, user3)
        followers = user3.followers.query().all()
        self.assertEqual(len(followers),1)
        self.assertEqual(followers[0],user1)
        user2.following.add(user3)
        followers = list(user3.followers.query())
        self.assertEqual(len(followers),2)

    def testFollowers(self):
        '''Add followers to a user'''
        # unwind queryset here since we are going to use it in a double loop
        models = self.mapper
        users = yield models.user.query().all()
        N = len(users)
        count = []
        # Follow users
        for user in users:
            N = self.data.followers()
            uset = set()
            for tofollow in populate('choice', N, choice_from=users):
                uset.add(tofollow)
                user.following.add(tofollow)
            count.append(len(uset))
            self.assertTrue(user.following.query().count()>0)
        #
        for user, N in zip(users, count):
            all_following = user.following.query()
            self.assertEqual(all_following.count(), N)
            for following in all_following:
                self.assertTrue(user in following.followers.query())

    def testFollowersTransaction(self):
        '''Add followers to a user'''
        # unwind queryset here since we are going to use it in a double loop
        models = self.mapper
        session = models.session()
        users = yield models.user.query(session).all()
        N = len(users)
        # Follow users
        with session.begin() as t:
            for user in users:
                self.assertEqual(user.session, session)
                N = self.data.followers()
                following = user.following
                for tofollow in populate('choice', N, choice_from=users):
                    following.add(tofollow)
        yield t.on_result
        for user in users:
            following = yield user.following.query().all()
            for user2 in following:
                group = yield user2.followers.query()
                self.assertTrue(user in group)

    def testMessages(self):
        models = self.mapper
        users = yield models.user.query().all()
        ids = [u.id for u in users]
        id = choice(ids)
        user = yield models.user.get(id=id)
        yield user.newupdate('this is my first message')
        yield user.newupdate('and this is another one')
        yield self.async.assertEqual(user.updates.size(), 2)

