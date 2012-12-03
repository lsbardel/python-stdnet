from datetime import datetime
from random import randint

from stdnet import odm
from stdnet.utils import test, populate, zip

from examples.models import User, Post

NUM_USERS = 50
MIN_FOLLOWERS = 5
MAX_FOLLOWERS = 10

usernames = populate('string',NUM_USERS, min_len = 5, max_len = 20)
passwords = populate('string',NUM_USERS, min_len = 8, max_len = 20)


class TestTwitter(test.CleanTestCase):
    models = (User, Post)

    def setUp(self):
        self.register()
        with User.objects.transaction() as t:
            for username,password in zip(usernames,passwords):
                t.add(User(username = username, password = password))
                
    def testMeta(self):
        following = User.following
        followers = User.followers
        self.assertEqual(following.formodel,User)
        self.assertEqual(following.relmodel,User)
        self.assertEqual(followers.formodel,User)
        self.assertEqual(followers.relmodel,User)
        through = following.through
        self.assertEqual(through,followers.through)
        self.assertEqual(len(through._meta.dfields),3)
        self.assertEqual(following.name_relmodel,'user')
        self.assertEqual(following.name_formodel,'user2')
        self.assertEqual(followers.name_relmodel,'user2')
        self.assertEqual(followers.name_formodel,'user')
        
    def testRelated(self):
        users = User.objects.query()
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
        users = list(User.objects.query())
        N = len(users)
        
        count = []
        # Follow users
        for user in users:
            n = randint(MIN_FOLLOWERS,MAX_FOLLOWERS)
            uset = set()
            for tofollow in populate('choice',n, choice_from = users):
                uset.add(tofollow)
                user.following.add(tofollow)
            count.append(len(uset))
            self.assertTrue(user.following.query().count()>0)
        
        for user,N in zip(users,count):
            all_following = user.following.query()
            self.assertEqual(all_following.count(),N)
            for following in all_following:
                self.assertTrue(user in following.followers.query())
                
    def testFollowersTransaction(self):
        '''Add followers to a user'''
        # unwind queryset here since we are going to use it in a double loop
        users = list(User.objects.query())
        N = len(users)
        
        # Follow users
        with User.objects.transaction() as t:
            for user in users:
                n = randint(MIN_FOLLOWERS,MAX_FOLLOWERS)
                following = user.following
                for tofollow in populate('choice',n, choice_from = users):
                    following.add(tofollow, transaction = t)
        
        for user in users:
            for following in user.following.query():
                self.assertTrue(user in following.followers.query())
            
    def testMessages(self):
        users = User.objects.query()
        N = len(users)
        id = randint(1,N)
        user = User.objects.get(id = id)
        user.newupdate('this is my first message')
        user.newupdate('and this is another one')
        user.save()
        self.assertEqual(user.updates.size(),2)
            
