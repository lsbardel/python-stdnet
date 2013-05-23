'''\
Stdnet provides a redis-based implementation for the
:class:`stdnet.odm.SearchEngine` so that you can have your models stored
and indexed in redis and if you like in the same redis instance.

Installing the search engine is as easy as

* Create a search engine and use the :meth:`stdnet.odm.Router.set_search_engine`
  method to install it in the router. Here, we use the
  :ref:`models router <tutorial-models-router>` of our first tutorial::

    from stdnet.apps.searchengine import SearchEngine
    
    models.set_search_engine(SearchEngine(backend, ...))
    
  where backend is either a instance of a :class:`stdnet.BackendDataServer`
  or a valid :ref:`connection string <connection-string>` such as::
  
      redis://127.0.0.1:6379?db=4

  or ``None``, to use the default :attr:`stdnet.odm.Router.backend`.
  This is the back-end server where the text indices :class:`WordItem`
  are stored, and not the back-end server of your models to index.
  They can be the same. To use the same backend as the default
  backend of the router::
  
    models.search_engine = SearchEngine(models.backend, ...)

* Register models you want to index to the search engine signletone::

    models.search_engine.register(Instrument)

  Check the :meth:`stdnet.odm.SearchEngine.register` documentation for more
  information.
  
Searching model instances for text can be achieved using the
:class:`stdnet.odm.Query.search` method::

    models.instrument.search('bla foo...')
    
Like all :class:`stdnet.odm.Query` methods, the search method can be chained
in an efficient way::

    models.instrument.filter(ccy='EUR').search('bla foo...')

API
==========

SearchEngine
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: SearchEngine
   :members:
   :member-order: bysource
   
   
WordItem
~~~~~~~~~~~~~~~~~~~~~~
   
 .. autoclass:: WordItem
   :members:
   :member-order: bysource
'''
import re
from inspect import isclass

from stdnet import odm, getdb

from .models import WordItem
from . import processors


class SearchEngine(odm.SearchEngine):
    """A python implementation for the :class:`stdnet.odm.SearchEngine`
driver.
    
:parameter min_word_length: minimum number of words required by the engine
                            to work.

                            Default ``3``.
                            
:parameter stop_words: list of words not included in the search engine.

                       Default ``stdnet.apps.searchengine.ignore.STOP_WORDS``
                          
:parameter metaphone: If ``True`` the double metaphone_ algorithm will be
    used to store and search for words. The metaphone should be the last
    world middleware to be added.
                      
    Default ``True``.

:parameter splitters: string whose characters are used to split text
                      into words. If this parameter is set to `"_-"`,
                      for example, than the word `bla_pippo_ciao-moon` will
                      be split into `bla`, `pippo`, `ciao` and `moon`.
                      Set to empty string for no splitting.
                      Splitting will always occur on white spaces.
                      
                      Default
                      ``stdnet.apps.searchengine.ignore.PUNCTUATION_CHARS``.

.. _metaphone: http://en.wikipedia.org/wiki/Metaphone
"""
    REGISTERED_MODELS = {}
    ITEM_PROCESSORS = []
    
    def __init__(self, backend=None, min_word_length=3, stop_words=None,
                 metaphone=True, stemming=True, splitters=None, **kwargs):
        super(SearchEngine, self).__init__(backend=backend, **kwargs)
        self.MIN_WORD_LENGTH = min_word_length
        splitters = splitters if splitters is not None else\
                    processors.PUNCTUATION_CHARS
        if splitters: 
            self.punctuation_regex = re.compile(\
                                    r"[%s]" % re.escape(splitters))
        else:
            self.punctuation_regex = None
        # The stop words middleware is only used for the indexing part
        self.add_word_middleware(processors.stopwords(stop_words), False)
        if stemming:
            self.add_word_middleware(processors.stemming_processor)
        if metaphone:
            self.add_word_middleware(processors.tolerant_metaphone_processor)
    
    def set_router(self, router):
        self.router = router
        router.register(WordItem, self.backend)
        
    def split_text(self, text):
        if self.punctuation_regex:
            text = self.punctuation_regex.sub(" ", text)
        mwl = self.MIN_WORD_LENGTH
        for word in text.split():
            if len(word) >= mwl:
                word = word.lower()
                yield word
    
    def flush(self):
        return self.router.worditem.flush()
        
    def add_item(self, item, words, transaction):
        for word in words:
            transaction.add(WordItem(word=word,
                                     model_type=item.__class__,
                                     object_id=item.id))
    
    def remove_item(self, item_or_model, transaction, ids=None):
        query = transaction.query(WordItem)
        if isclass(item_or_model):
            wi = query.filter(model_type=item_or_model)
            if ids is not None:
                wi = wi.filter(object_id=ids)
        else:
            wi = query.filter(model_type=item_or_model.__class__,
                              object_id=item_or_model.id)
        transaction.delete(wi)
    
    def search(self, text, include=None, exclude=None, lookup=None):
        words = self.words_from_text(text, for_search=True)
        return self._search(words, include, exclude, lookup)
    
    def search_model(self, q, text, lookup=None):
        '''Implements :meth:`stdnet.odm.SearchEngine.search_model`.
It return a new :class:`stdnet.odm.QueryElem` instance from
the input :class:`Query` and the *text* to search.'''
        words = self.words_from_text(text, for_search=True)
        if not words:
            return q
        qs = self._search(words, include = (q.model,), lookup = lookup)
        qs = tuple((q.get_field('object_id') for q in qs))
        return odm.intersect((q,)+qs)
    
    def worditems(self, model=None):
        q = self.router.worditem.query()
        if model:
            if not isclass(model):
                return q.filter(model_type=model.__class__, object_id=model.id)
            else:
                return q.filter(model_type=model)
        else:
            return q
    
    def _search(self, words, include=None, exclude=None, lookup=None):
        '''Full text search. Return a list of queries to intersect.'''
        lookup = lookup or 'contains'
        query = self.router.worditem.query()
        if include:
            query = query.filter(model_type__in=include)
        if exclude:
            query = query.exclude(model_type__in=include)
        if not words:
            return [query]
        qs = []
        if lookup == 'in':
            # we are looking for items with at least one word in it
            qs.append(query.filter(word__in=words))
        elif lookup == 'contains':
            #we want to match every single words
            for word in words:
                qs.append(query.filter(word=word))
        else:
            raise ValueError('Unknown lookup "{0}"'.format(lookup))
        return qs    