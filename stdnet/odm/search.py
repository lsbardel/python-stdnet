import logging

from itertools import chain
from inspect import isgenerator
from datetime import datetime

from .models import StdModel
from .fields import DateTimeField, CharField
from .signals import post_commit, post_delete


logger = logging.getLogger('stdnet.search')

class SearchEngine(object):
    """Stdnet search engine driver. This is an abstract class which
expose the base functionalities for full text-search on model instances.
Stdnet also provides a :ref:`python implementation <apps-searchengine>`
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
"""
    REGISTERED_MODELS = {}
    ITEM_PROCESSORS = []
    last_indexed = 'last_indexed'

    def __init__(self):
        self.word_middleware = []
        self.add_processor(stdnet_processor())

    def register(self, model, related=None):
        '''Register a :class:`StdModel` to the search engine.
When registering a model, every time an instance is created, it will be
indexed by the search engine.

:parameter model: a :class:`StdModel` class.
:parameter related: a list of related fields to include in the index.
'''
        model._meta.searchengine = self
        model._index_related = related or ()
        update_model = UpdateSE(self)
        self.REGISTERED_MODELS[model] = update_model
        post_commit.connect(update_model, sender = model)
        post_delete.connect(update_model, sender = model)

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

    def index_item(self, item, transaction):
        """This is the main function for indexing items.
It extracts content from the given *item* and add it to the index.

:parameter item: an instance of a :class:`stdnet.odm.StdModel`.
"""
        self.index_items_from_model((item,), item.__class__, transaction)
            
    def index_items_from_model(self, items, model, transaction):
        """This is the main function for indexing items.
It extracts content from a list of *items* belonging to *model* and
add it to the index.

:parameter items: an iterable over instances of of a :class:`stdnet.odm.StdModel`.
:parameter model: The *model* of all *items*.
:parameter transaction: A transaction for updauing indexes.
"""
        ids = []
        wft = self.words_from_text
        add = self.add_item
        for item, data in self._item_data(items):
            ids.append(item.id)
            words = chain(*[wft(value) for value in data])
            add(item, words, transaction)
        if ids:
            self.remove_item(model, transaction, ids)

    def reindex(self):
        '''Re-index models by removing items in
:class:`stdnet.contrib.searchengine.WordItem` and rebuilding them by iterating
through all the instances of model provided.
If models are not provided, it reindex all models registered
with the search engine.'''
        self.flush()
        n = 0
        # Loop over models
        for model in self.REGISTERED_MODELS:
            # get all fiels to index
            fields = tuple((f.name for f in model._meta.scalarfields\
                            if f.type == 'text'))
            session = self.session()
            with session.begin():
                for obj in model.objects.query().load_only(*fields):
                    n += 1
                    self.index_item(obj, session)
        return n

    # INTERNALS
    #################################################################

    def item_field_iterator(self, item):
        for processor in self.ITEM_PROCESSORS:
            result = processor(item)
            if result is not None:
                return result
        raise ValueError(
                'Cound not iterate through item {0} fields'.format(item))
        
    def _item_data(self, items):
        fi = self.item_field_iterator
        for item in items:
            data = fi(item)
            if data:
                yield item, data
        

    # ABSTRACT FUNCTIONS
    ################################################################

    def session(self):
        '''Create a session for the search engine'''
        return None

    def remove_item(self, item_or_model, session, ids=None):
        '''Remove an item from the search indices'''
        raise NotImplementedError()

    def add_item(self, item, words, session):
        '''Create indices for *item* and each word in *words*.

:parameter item: a *model* instance to be indexed. It does not need to be
    a :class:`stdnet.odm.StdModel`.
:parameter words: iterable over words. This iterable has been obtained from the
    text in *item* via the :attr:`word_middleware`.
'''
        raise NotImplementedError()

    def search(self, text, include = None, exclude = None, lookup = None):
        raise NotImplementedError()

    def search_model(self, query, text, lookup = None):
        '''Search *text* in *model* instances. This is the functions
needing implementation by custom serach engines.

:parameter query: a :class:`Query` on a :class:`StdModel`.
:parameter text: text to search
:parameter lookup: Optional lookup, one of ``contains`` or ``in``.
:rtype: An updated :class:`Query`.'''
        raise NotImplementedError()

    def flush(self, full = False):
        '''Clean the search engine'''
        raise NotImplementedError()


class UpdateSE(object):

    def __init__(self, se):
        self.se = se

    def __call__(self, instances, session=None, signal=None, sender=None,
                 **kwargs):
        '''An update on instances has occured. Propagate it to the search
engine index models.'''
        if session is None:
            raise ValueError('No session available. Cannot updated indexes.')
        if sender:
            if signal == post_delete:
                self.remove(instances, session, sender)
            else:
                self.index(instances, session, sender)

    def index(self, instances, session, sender):
        # The session is not in a transaction since this is a callback
        logger.debug('indexing %s instances of %s',
                     len(instances), sender._meta)
        with session.begin(name='Index search engine') as t:
            self.se.index_items_from_model(instances, sender, t)

    def remove(self, instances, session, sender):
        logger.debug('Removing from search index %s instances of %s',
                     len(instances), sender._meta)
        remove_item = self.se.remove_item
        with session.begin(name='Remove search indexes') as t:
            remove_item(sender, t, instances)


class stdnet_processor(object):
    '''A search engine processor for stdnet models.
An engine processor is a callable
which return an iterable over text.'''
    def __call__(self, item):
        if isinstance(item, StdModel):
            return self.field_iterator(item)

    def field_iterator(self, item):
        related = getattr(item, '_index_related', ())
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
                    data.extend(self.field_iterator(value))
        return data
