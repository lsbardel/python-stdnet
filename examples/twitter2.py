from datetime import datetime
from stdnet import orm

class Post(orm.StdModel):
    
    def __init__(self, data = ''):
        self.dt   = datetime.now()
        self.data = data
        super(Post,self).__init__()
    
    
class User(orm.StdModel):
    '''A model for holding information about users'''
    username  = orm.AtomField(unique = True)
    password  = orm.AtomField()
    updates   = orm.ListField()
    
    def __unicode__(self):
        return self.username
    
    def newupdate(self, data):
        p  = Post(data = data).save()
        self.updates.push_front(p.id)
        return p
    
    def follow(self, user):
        '''Follow a user'''
        if user is not self and user not in self.following():
            return UserFollower(user = user, follower = self).save()
        
    def following(self):
        return set(f.user for f in self.following_set.all())
    
    def followers(self):
        return set(f.user for f in self.followers_set.all())
        
        
class UserFollower(orm.StdModel):
    user = orm.ForeignKey(User, related_name = 'followers_set')
    follower = orm.ForeignKey(User, related_name = 'following_set')
    
