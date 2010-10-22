.. _twitter-example:

==============================
Twitter Clone Example
==============================

A very simple twitter clone implemented using ``stdnet`` library.
Illustrates the use of :class:`stdnet.orm.orm.ManyToManyField` and
:class:`stdnet.orm.orm.ListField`::

	from datetime import datetime
	from stdnet import orm
	
	class Post(orm.StdModel):
	    dt   = orm.DateField(index = False)
	    data = orm.CharField()
	    user = orm.ForeignKey("User")
	    
	    def __init__(self, data = '', dt = None):
	        dt   = dt or datetime.now()
	        super(Post,self).__init__(data = data, dt = dt)
	    
	    
	class User(orm.StdModel):
	    '''A model for holding information about users'''
	    username  = orm.SymbolField(unique = True)
	    password  = orm.CharField(required = True)
	    updates   = orm.ListField(model = Post)
	    following = orm.ManyToManyField(model = 'self', related_name = 'followers')
	    
	    def __str__(self):
	        return self.username
	    
	    def newupdate(self, data):
	        p  = Post(data = data, user = self).save()
	        self.updates.push_front(p)
	        return p
	    
	    
These models are available in the :mod:`stdnet.tests` module.
We can import them by using::

	from stdnet.tests.examples.models import Post, User
	
Before using the models, we need to register them to a back-end. If your redis server is running locally
just type::

	>>> from stdnet import orm
	>>> orm.register(User)
	'redis'
	>>> orm.register(Post)
	'redis'
	
Now lets try it out::

	>>> u = User(username='pluto', password='bla')
	>>> u.save()
	User: pluto
	
Ok we have a user. Lets add few updates::

	>>> u.newupdate('my name is Luka and I live on second floor)
	>>> u.newupdate('ciao')
	>>> u.save()
	>>> u.updates.size()
	2
	>>> for p in u.updates:
	...     print p.data
	... 
	my name is Luka and I live on second floor
	ciao
	>>>
	