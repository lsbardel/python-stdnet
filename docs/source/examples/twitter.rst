.. _twitter-example:

==============================
A twitter clone
==============================

This is the stdnet equivalent of the `redis twitter clone`_ example.
It illustrates the use of :class:`stdnet.odm.ManyToManyField` and
:ref:`implicit sorting <implicit-sorting>`::

	from datetime import datetime
	from stdnet import odm
	
	class Post(odm.StdModel):
	    timestamp = odm.DateTimeField(default = datetime.now)
	    data = odm.CharField()
	    user = odm.ForeignKey("User")
	    
	    def __unicode__(self):
	        return self.data
	        
	    class Meta:
	       ordering = '-timestamp'
    
    
	class User(odm.StdModel):
	    '''A model for holding information about users'''
	    username  = odm.SymbolField(unique = True)
	    password  = odm.CharField(required = True)
	    following = odm.ManyToManyField(model = 'self',
	                                    related_name = 'followers')
	    
	    def __unicode__(self):
	        return self.username
	    
	    def newupdate(self, data):
	        return Post(data = data, user = self).save()
	    
	    
These models are available in the :mod:`stdnet.tests` module.
We can import them by using::

	from stdnet.tests.examples.models import Post, User
	
Before using the models, we need to register them to a back-end.
If your redis server is running locally
just type::

	>>> from stdnet import odm
	>>> odm.register(User)
	'redis db 7 on 127.0.0.1:6379'
	>>> odm.register(Post)
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
	

.. _redis twitter clone: http://redis.io/topics/twitter-clone
