import logging

from itertools import chain
from inspect import isgenerator, isclass

from stdnet.utils import grouper
from stdnet.utils.async import async


LOGGER = logging.getLogger('stdnet.search')


class SearchEngine(object):
    """Stdnet search engine driver. This is an abstract class which
expose the base functionalities for full text-search on model instances.
Stdnet also provides a :ref:`python implementation <tutorial-search>`
of this interface.

The main methods to be implemented are :meth:`add_item`,
:meth:`remove_index` and :meth:`search_model`.

.. attribute:: word_middleware

    A list of middleware functions for preprocessing text
    to be indexed. A middleware function has arity 1 by
    accepting an iterable of words and
    returning an iterable of words. Word middleware functions
    are added to the search engine via the
    :meth:`add_word_middleware` method.

    For example this function remove a group of words from the index::

        se = SearchEngine()

        class stopwords(object):

            def __init__(self, *swords):
                self.swords = set(swords)

            def __call__(self, words):
                for word in words:
                    if word not in self.swords:
                        yield word

        se.add_word_middleware(stopwords('and','or','this','that',...))
        
.. attribute:: max_in_session

    Maximum number of instances to be reindexed in one session.
    Default ``1000``.
"""
    def __init__(self, backend=None, logger=None, max_in_session=None):
        self._backend = backend
        self.REGISTERED_MODELS = {}
        self.ITEM_PROCESSORS = []
        self.last_indexed = 'last_indexed'
        self.word_middleware = []
        self.add_processor(stdnet_processor(self))
        self.logger = logger or LOGGER
        self.max_in_session = max_in_session or 1000
        self.router = None

    @property
    def backend(self):
        '''Backend for this search engine.'''
        return self._backend
    
    def register(self, model, related=None):
        '''Register a :class:`StdModel` with this search :class:`SearchEngine`.
When registering a model, every time an instance is created, it will be
indexed by the search engine.

:parameter model: a :class:`StdModel` class.
:parameter related: a list of related fields to include in the index.
'''
        update_model = UpdateSE(self, related)
        self.REGISTERED_MODELS[model] = update_model
        self.router.post_commit.connect(update_model, sender=model)
        self.router.post_delete.connect(update_model, sender=model)
        
    def get_related_fields(self, item):
        if not isclass(item):
            item = item.__class__
        registered = self.REGISTERED_MODELS.get(item)
        return registered.related if registered else ()

    def words_from_text(self, text, for_search=False):
        '''Generator of indexable words in *text*.
This functions loop through the :attr:`word_middleware` attribute
to process the text.

:parameter text: string from which to extract words.
:parameter for_search: flag indicating if the the words will be used for search
    or to index the database. This flug is used in conjunction with the
    middleware flag *for_search*. If this flag is ``True`` (i.e. we need to
    search the database for the words in *text*), only the
    middleware functions in :attr:`word_middleware` enabled for searching are
    used.

    Default: ``False``.

return a *list* of cleaned words.
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

    def add_word_middleware(self, middleware, for_search=True):
        '''Add a *middleware* function to the list of :attr:`word_middleware`,
for preprocessing words to be indexed.

:parameter middleware: a callable receving an iterable over words.
:parameter for_search: flag indicating if the *middleware* can be used for the
    text to search. Default: ``True``.
'''
        if hasattr(middleware,'__call__'):
            self.word_middleware.append((middleware,for_search))

    def index_item(self, item):
        """This is the main function for indexing items.
It extracts content from the given *item* and add it to the index.

:parameter item: an instance of a :class:`stdnet.odm.StdModel`.
"""
        self.index_items_from_model((item,), item.__class__)
            
    @async()
    def index_items_from_model(self, items, model):
        """This is the main function for indexing items.
It extracts content from a list of *items* belonging to *model* and
add it to the index.

:parameter items: an iterable over instances of of a :class:`stdnet.odm.StdModel`.
:parameter model: The *model* of all *items*.
:parameter transaction: A transaction for updating indexes.
"""
        self.logger.debug('Indexing %s objects of %s model.',
                          len(items), model._meta)
        session = self.session()
        wft = self.words_from_text
        add = self.add_item
        total = 0
        for group in grouper(self.max_in_session, items):
            with session.begin() as transaction:
                ids = []
                for item, data in self._item_data(group):
                    ids.append(item.id)
                    words = chain(*[wft(value) for value in data])
                    add(item, words, transaction)
                if ids:
                    total += len(ids)
                    self.remove_item(model, transaction, ids)
            yield transaction.on_result
        yield total

    def query(self, model):
        '''Return a query for ``model`` when it needs to be indexed.'''
        session = self.router.session()
        fields = tuple((f.name for f in model._meta.scalarfields\
                         if f.type=='text'))
        qs = session.query(model).load_only(*fields)
        for related in self.get_related_fields(model):
            qs = qs.load_related(related)
        return qs
        
    @async()
    def reindex(self):
        '''Re-index models by removing indexes and rebuilding them by iterating
through all the instances of :attr:`REGISTERED_MODELS`.'''
        yield self.flush()
        total = 0
        # Loop over models
        for model in self.REGISTERED_MODELS:
            all = yield self.query(model).all()
            if all:
                n = yield self.index_items_from_model(all, model)
                total += n
        yield total

    def session(self):
        '''Create a session for the search engine'''
        return self.router.session()
    
    # INTERNALS
    #################################################################
    def set_router(self, router):
        self.router = router
        
    def item_field_iterator(self, item):
        if item:
            for processor in self.ITEM_PROCESSORS:
                result = processor(item)
                if result is not None:
                    return result
        raise ValueError('Cound not iterate through "%s" fields' % item)
        
    def _item_data(self, items):
        fi = self.item_field_iterator
        for item in items:
            if item is None:    # stop if we get a None
                break
            data = fi(item)
            if data:
                yield item, data
        
    # ABSTRACT FUNCTIONS
    ################################################################
    def remove_item(self, item_or_model, session, ids=None):
        '''Remove an item from the search indices'''
        raise NotImplementedError

    def add_item(self, item, words, transaction):
        '''Create indices for *item* and each word in *words*. Must be
implemented by subclasses.

:parameter item: a *model* instance to be indexed. It does not need to be
    a :class:`stdnet.odm.StdModel`.
:parameter words: iterable over words. It has been obtained from the
    text in *item* via the :attr:`word_middleware`.
:param transaction: The :class:`Transaction` used.
'''
        raise NotImplementedError

    def search(self, text, include=None, exclude=None, lookup=None):
        '''Full text search. Must be implemented by subclasses.

:param test: text to search
:param include: optional list of models to include in the search. If not
    provided all :attr:`REGISTERED_MODELS` will be used.
:param exclude: optional list of models to exclude for the search.
:param lookup: currently not used.'''
        raise NotImplementedError

    def search_model(self, query, text, lookup=None):
        '''Search *text* in *model* instances. This is the functions
needing implementation by custom serach engines.

:parameter query: a :class:`Query` on a :class:`StdModel`.
:parameter text: text to search
:parameter lookup: Optional lookup, one of ``contains`` or ``in``.
:rtype: An updated :class:`Query`.'''
        raise NotImplementedError

    def flush(self, full=False):
        '''Clean the search engine'''
        raise NotImplementedError


class UpdateSE(object):

    def __init__(self, se, related=None):
        self.se = se
        self.related = related or ()

    def __call__(self, instances, signal, sender, **kwargs):
        '''An update on instances has occurred. Propagate it to the search
engine index models.'''
        if sender:
            # get a new session
            models = self.se.router
            se_session = models.session()
            if signal == models.post_delete:
                return self.remove(instances, sender, se_session)
            else:
                return self.index(instances, sender, se_session)

    def index(self, instances, sender, session):
        return self.se.index_items_from_model(instances, sender)

    def remove(self, instances, sender, session):
        self.se.logger.debug('Removing from search index %s instances of %s',
                             len(instances), sender._meta)
        remove_item = self.se.remove_item
        with session.begin(name='Remove search indexes') as t:
            remove_item(sender, t, instances)
        return t.on_result


class stdnet_processor(object):
    '''A search engine processor for stdnet models.
An engine processor is a callable
which return an iterable over text.'''
    def __init__(self, se):
        self.se = se
        
    def __call__(self, item):
        related = self.se.get_related_fields(item)
        data = []
        for field in item._meta.fields:
            if field.hidden:
                continue
            if field.type == 'text':
                if hasattr(item, field.attname):
                    data.append(getattr(item, field.attname))
                else:
                    return ()
            elif field.name in related:
                value = getattr(item, field.name, None)
                if value:
                    data.extend(self.se.item_field_iterator(value))
        return data
