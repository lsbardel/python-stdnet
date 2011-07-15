from itertools import chain
from inspect import isgenerator

from .models import StdModel
from .signals import post_save, post_delete


class SearchEngine(object):
    """Stdnet search engine driver.
    
:attribute word_middleware: a list of functions for preprocessing text
                            to be indexed. The first middleware function
                            must accept a string and return an iterable over
                            words. All the other must accept iterable and
                            return iterable.
"""
    REGISTERED_MODELS = {}
    ITEM_PROCESSORS = []
    
    def __init__(self):
        self.word_middleware = []
        self.add_processor(stdnet_processor())
        
    def register(self, model):
        '''Register a model to the search engine. By registering a model,
every time an instance is updated or created, it will be indexed by the
search engine.

:parameter model: a :class:`stdnet.orm.StdModel` class.
'''
        if model not in self.REGISTERED_MODELS:
            update_model = UpdateSE(self)
            delete_model = RemoveFromSE(self)
            self.REGISTERED_MODELS[model] = (update_model,delete_model)
            post_save.connect(update_model, sender = model)
            post_delete.connect(delete_model, sender = model)
            
    def words_from_text(self, text):
        '''Generator of indexable words in *text*.
This functions loop through the :attr:`word_middleware` middleware
to process the text.

:parameter text: string from which to extract words.
'''
        if not text:
            raise StopIteration
        
        word_gen = self.split_text(text)
        
        for middleware in self.word_middleware:
            word_gen = middleware(word_gen)
        
        if isgenerator(word_gen):
            word_gen = list(word_gen)
            
        return word_gen
    
    def split_text(self, text):
        '''Split text into words and return an iterable over them.
Can and should be '''
        return text.split()
    
    def add_processor(self, processor):
        if processor not in self.ITEM_PROCESSORS:
            self.ITEM_PROCESSORS.append(processor)

    def add_word_middleware(self, middleware):
        if hasattr(middleware,'__call__'):
            self.word_middleware.append(middleware)
    
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

    
    def remove_item(self, item):
        '''Remove an item from the serach indices'''
        raise NotImplementedError
    
    def search_model(self, model, text):
        '''Return a query for ids of model instances containing
words in text.'''
        raise NotImplementedError

    # ABSTRACT INTERNAL FUNCTIONS
    
    def _index_item(self, item, words):
        raise NotImplementedError
    
    
class UpdateSE(object):
    
    def __init__(self, se):
        self.se = se
        
    def __call__(self, instance, **kwargs):
        self.se.index_item(instance)
        
        
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
        for field in item._meta.fields:
            if field.type == 'text':
                value = getattr(item,field.attname)
                if value:
                    yield value
