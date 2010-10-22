from datetime import datetime
from itertools import izip
from random import randint

from stdnet.test import TestCase
from stdnet.utils import populate

from examples.models import User, Post

NUM_USERS = 100
MIN_FOLLOWERS = 10
MAX_FOLLOWERS = 30

usernames = populate('string',NUM_USERS, min_len = 5, max_len = 20)
passwords = populate('string',NUM_USERS, min_len = 8, max_len = 20)


class TestTwitter(TestCase):

    def setUp(self):
        self.orm.register(User)
        self.orm.register(Post)
        for username,password in izip(usernames,passwords):
            User(username = username, password = password).save(False)
        User.commit()
    
    def testFollowers(self):
        '''Add followers to a user'''
        users = User.objects.all()
        N = users.count()
        
        # Follow users
        for user in users:
            n = randint(MIN_FOLLOWERS,MAX_FOLLOWERS)
            for tofollow in populate('choice',n, choice_from = users):
                if tofollow.id != user.id:
                    user.following.add(tofollow)
            user.save()
            self.assertTrue(user.following.size()>0)
        
        for user in users:
            for following in user.following:
                self.assertTrue(user in following.followers)
            
    def testMessages(self):
        users = User.objects.all()
        N = len(users)
        id = randint(1,N)
        user = User.objects.get(id = id)
        user.newupdate('this is my first message')
        user.newupdate('and this is another one')
        user.updates.save()
        self.assertEqual(user.updates.size(),2)
            
        
            