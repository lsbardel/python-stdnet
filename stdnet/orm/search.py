from itertools import chain
from inspect import isgenerator
from datetime import datetime

from .models import StdModel
from .fields import DateTimeField, CharField
from .signals import post_save, post_delete


class SearchEngine(object):
    """Stdnet search engine driver. This is an abstract class which
expose the base functionalities for full text-search on model instances.
    
.. attribute:: word_middleware
    
    A list of functions for preprocessing text
    to be indexed. Middleware function
    must accept an iterable of words and
    return iterable of words. Word middleware functions
    are added to the search engine via the
    :meth:`stdnet.orm.SearchEngine.add_word_middleware`. For example
    this function remove a group of words from the index::
    
        se = SearchEngine()
        
        def stopwords(words):
            for word in words:
                if word not in ('this','that','and'):
                    yield word
        
        se.add_word_middleware(stopwords)
"""
    REGISTERED_MODELS = {}
    ITEM_PROCESSORS = []
    last_indexed = 'last_indexed'
    
    def __init__(self):
        self.word_middleware = []
        self.add_processor(stdnet_processor())
        
    def register(self, model, related = None, tag_field = 'tags'):
        '''Register a model to the search engine. By registering a model,
every time an instance is created, it will be indexed by the
search engine.

:parameter model: a :class:`stdnet.orm.StdModel` class.
:parameter related: a list of related fields to include in the index.
'''
        model._meta.searchengine = self
        if self.last_indexed not in model._meta.dfields:
            field = DateTimeField(required = False)
            field.register_with_model('last_indexed',model)
        if tag_field:
            field = CharField()
            field.register_with_model(tag_field,model)
        model._tag_field = tag_field
        model._index_related = related or ()
        update_model = UpdateSE(self)
        delete_model = RemoveFromSE(self)
        self.REGISTERED_MODELS[model] = (update_model,delete_model)
        post_save.connect(update_model, sender = model)
        post_delete.connect(delete_model, sender = model)
            
    def words_from_text(self, text, for_search = False):
        '''Generator of indexable words in *text*.
This functions loop through the :attr:`word_middleware` attribute
to process the text.

:parameter text: string from which to extract words.

return a list of cleaned words.
'''
        if not text:
            return []
        
        word_gen = self.split_text(text)
        
        for middleware,fors in self.word_middleware:
            if for_search and not fors:
                continue
            word_gen = middleware(word_gen)
        
        if isgenerator(word_gen):
            word_gen = list(word_gen)
            
        return word_gen
    
    def split_text(self, text):
        '''Split text into words and return an iterable over them.
Can and should be reimplemented by subclasses.'''
        return text.split()
    
    def add_processor(self, processor):
        if processor not in self.ITEM_PROCESSORS:
            self.ITEM_PROCESSORS.append(processor)

    def add_word_middleware(self, middleware, for_search = True):
        '''Add a *middleware* function for preprocessing words to be indexed.'''
        if hasattr(middleware,'__call__'):
            self.word_middleware.append((middleware,for_search))
    
    def index_item(self, item, skipremove = False):
        """This is the main function for indexing items.
It extracts content from the given *item* and add it to the index.
If autocomplete is enabled, it adds indexes for it too.

:parameter item: an instance of a :class:`stdnet.orm.StdModel`.
:parameter skipremove: If ``True`` it skip the remove step for
                       improved performance.
                       
                       Default ``False``.
"""
        if not skipremove:
            self.remove_item(item)
        wft = self.words_from_text
        words = chain(*[wft(value) for value in\
                            self.item_field_iterator(item)])                
        wc = {}
        for word in words:
            if word in wc:
                wc[word] += 1
            else:
                wc[word] = 1
        
        return self._index_item(item,wc)

    def flush(self, full = False):
        '''Clean the search engine'''
        raise NotImplementedError
    
    def remove_item(self, item):
        '''Remove an item from the serach indices'''
        raise NotImplementedError
    
    def search_model(self, model, text):
        '''Return a query for ids of model instances containing
words in text.'''
        raise NotImplementedError
    
    def reindex(self, full = True):
        '''Reindex models by removing items in
:class:`stdnet.contrib.searchengine.WordItem` and rebuilding them by iterating
through all the instances of model provided.
If models are not provided, it reindex all models registered
with the search engine.'''
        self.flush(full)
        n = 0
        for model in self.REGISTERED_MODELS:
            for obj in model.objects.all():
                n += 1
                self.index_item(obj,True)
        return n

    # ABSTRACT INTERNAL FUNCTIONS
    ################################################################
    
    def _index_item(self, item, words):
        raise NotImplementedError
    
    
class UpdateSE(object):
    
    def __init__(self, se):
        self.se = se
        
    def __call__(self, instance, **kwargs):
        if not instance.last_indexed:
            self.se.index_item(instance)
            instance.last_indexed = datetime.now()
            instance.save(skip_signal = True)
        
        
class RemoveFromSE(object):
    
    def __init__(self, se):
        self.se = se
        
    def __call__(self, instance, **kwargs):
        self.se.remove_item(instance)
        

class stdnet_processor(object):
    '''A search engine processor for stdnet models.
An engine processor is a callable
which return an iterable over text.'''
    def __call__(self, item):
        if isinstance(item,StdModel):
            return self.field_iterator(item)
    
    def field_iterator(self, item):
        related = getattr(item,'_index_related',())
        for field in item._meta.fields:
            if field.hidden:
                continue
            if field.type == 'text':
                value = getattr(item,field.attname)
                if value:
                    yield value
            elif field.name in related:
                value = getattr(item,field.name,None)
                if value:
                    for value in self.field_iterator(value):
                        yield value
