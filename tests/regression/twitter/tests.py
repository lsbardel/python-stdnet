from datetime import datetime
from random import randint

from stdnet.test import TestCase
from stdnet.utils import populate, zip

from examples.models import User, Post

NUM_USERS = 50
MIN_FOLLOWERS = 5
MAX_FOLLOWERS = 10

usernames = populate('string',NUM_USERS, min_len = 5, max_len = 20)
passwords = populate('string',NUM_USERS, min_len = 8, max_len = 20)


class TestTwitter(TestCase):

    def setUp(self):
        self.orm.register(User)
        self.orm.register(Post)
        with User.transaction() as t:
            for username,password in zip(usernames,passwords):
                User(username = username, password = password).save(t)
        
    def testRelated(self):
        users = User.objects.all()
        user1 = users[0]
        user2 = users[1]
        user3 = users[2]
        user1.following.add(user3)
        followers = list(user3.followers.all())
        self.assertEqual(len(followers),1)
        user2.following.add(user3)
        followers = list(user3.followers.all())
        self.assertEqual(len(followers),2)
    
    def testFollowers(self):
        '''Add followers to a user'''
        # unwind queryset here since we are going to use it in a double loop
        users = list(User.objects.all())
        N = len(users)
        
        # Follow users
        for user in users:
            n = randint(MIN_FOLLOWERS,MAX_FOLLOWERS)
            for tofollow in populate('choice',n, choice_from = users):
                user.following.add(tofollow)
            self.assertTrue(user.following.all().count()>0)
        
        for user in users:
            for following in user.following.all():
                self.assertTrue(user in following.followers.all())
            
    def testMessages(self):
        users = User.objects.all()
        N = len(users)
        id = randint(1,N)
        user = User.objects.get(id = id)
        user.newupdate('this is my first message')
        user.newupdate('and this is another one')
        user.save()
        self.assertEqual(user.updates.size(),2)
            
