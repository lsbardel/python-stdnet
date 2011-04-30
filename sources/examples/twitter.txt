.. _twitter-example:

==============================
Twitter Clone Example
==============================

A very simple twitter clone implemented using ``stdnet`` library.
Illustrates the use of :class:`stdnet.orm.ManyToManyField` and
:class:`stdnet.orm.ListField`::

	from datetime import datetime
	from stdnet import orm
	
	class Post(orm.StdModel):
	    dt   = orm.DateField(index = False)
	    data = orm.CharField()
	    user = orm.ForeignKey("User")
	    
	    def __unicode__(self):
	        return self.data
	    
	    
	class User(orm.StdModel):
	    '''A model for holding information about users'''
	    username  = orm.SymbolField(unique = True)
	    password  = orm.CharField(required = True)
	    updates   = orm.ListField(model = Post)
	    following = orm.ManyToManyField(model = 'self', related_name = 'followers')
	    
	    def __unicode__(self):
	        return self.username
	    
	    def newupdate(self, data):
	        p  = Post(data = data, user = self, dt = datetime.now()).save()
	        self.updates.push_front(p)
	        return p
	    
	    
These models are available in the :mod:`stdnet.tests` module.
We can import them by using::

	from stdnet.tests.examples.models import Post, User
	
Before using the models, we need to register them to a back-end. If your redis server is running locally
just type::

	>>> from stdnet import orm
	>>> orm.register(User)
	'redis db 7 on 127.0.0.1:6379'
	>>> orm.register(Post)
	'redis db 7 on 127.0.0.1:6379'
	
Now lets try it out::

	>>> u = User(username='pluto', password='bla')
	>>> u.save()
	User: pluto
	
Ok we have a user. Lets add few updates::

	>>> u.newupdate('my name is Luka and I live on second floor')
	Post: my name is Luka and I live on second floor
	>>> u.newupdate('ciao')
	Post: ciao
	>>> u.save()
	User: pluto
	>>> u.updates.size()
	2
	>>> for p in u.updates:
	...     print('%s :  %s' % (p.dt,p))
	... 
	2010-11-10 18:05:59 :  ciao
	2010-11-10 18:05:24 :  my name is Luka and I live on second floor
	>>>
	