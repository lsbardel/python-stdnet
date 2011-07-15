

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
            orm.post_save.connect(update_model, sender = model)
            orm.post_delete.connect(delete_model, sender = model)

    def words_from_text(self, text):
        '''Generator of indexable words in *text*.
This functions loop through the :attr:`word_middleware` middleware
to process the text.

:parameter text: string from which to extract words.
'''
        if not text:
            raise StopIteration
        
        if self.word_middleware:
            word_gen = text
            for middleware in self.word_middleware:
                word_gen = middleware(text)
        else:
            word_gen = text.split()
        
        return word_gen
    