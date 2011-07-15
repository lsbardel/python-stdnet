'''\
An application for building a search-engine on ``stdnet`` models.

Usage
===========

Somewhere in your application create the search engine singletone::

    from stdnet.contrib.searchengine import SearchEngine
     
    engine = SearchEngine(...)
 
The engine works by registering models to it or by indexing instances. If
you go down the registering route, the simplest one, everything is done for
you, all you need to do is to register a model with the search engine. 
For example::

    engine.register(MyModel)

From now on, every time and instance of ``MyModel`` is updated/created,
the search engine will updated its indexes.

To search, issue the command::

    search_result = engine.search(sometext)
    
If you would like to limit the search to some specified models::

    search_result = engine.search(sometext, include = (model1,model2,...))
'''
import re
from itertools import chain
from inspect import isclass

from stdnet import orm
from stdnet.utils import to_string, iteritems

from .models import Word, WordItem, AutoComplete
from .ignore import STOP_WORDS, PUNCTUATION_CHARS
from .metaphone import dm as double_metaphone


class SearchEngine(object):
    """Search engine driver.
Adapted from
https://gist.github.com/389875
    
:parameter min_word_length: minimum number of words required by the engine
                            to work.

                            Default ``3``.
                            
:parameter stop_words: list of words not included in the search engine.

                       Default ``stdnet.contrib.searchengine.ignore.STOP_WORDS``
                        
:parameter autocomplete: Name for the autocomplete sorted set.
                         If ``None`` `autocomplete` functionality won't
                         be available.
                         
                         Default ``None``.
                         
:parameter metaphone: If ``True`` the double metaphone_ algorithm will be
                      used to store and search for words.
                      
                      Default ``True``.

:parameter splitters: string whose characters are used to split text
                      into words. If this parameter is set to `"_-"`,
                      for example, than the word `bla_pippo_ciao-moon` will
                      be split into `bla`, `pippo`, `ciao` and `moon`.
                      Set to empty string for no splitting.
                      Splitting will always occur on white spaces.
                      
                      Default
                      ``stdnet.contrib.searchengine.ignore.PUNCTUATION_CHARS``.

.. _metaphone: http://en.wikipedia.org/wiki/Metaphone
"""
    REGISTERED_MODELS = {}
    ITEM_PROCESSORS = []
    
    def __init__(self, min_word_length = 3, stop_words = None,
                 autocomplete = None, metaphone = True,
                 splitters = None):
        self.MIN_WORD_LENGTH = min_word_length
        self.STOP_WORDS = stop_words if stop_words is not None else STOP_WORDS
        splitters = splitters if splitters is not None else PUNCTUATION_CHARS
        if splitters: 
            self.punctuation_regex = re.compile(\
                                    r"[%s]" % re.escape(splitters))
        else:
            self.punctuation_regex = None
        self.metaphone = metaphone
        self._autocomplete = autocomplete
        self.add_processor(stdnet_processor())           
        
    @property
    def autocomplete(self):
        if self._autocomplete:
            ac = AutoComplete.me(self._autocomplete)
            #ac.minlen = self.MIN_WORD_LENGTH
            return ac
        
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
        wft = self.get_words_from_text
        link = self._link_item_and_word
        
        words = list(chain(*[wft(value) for value in\
                              self.item_field_iterator(item)]))
        linked = []
        auto = self.autocomplete
        if auto:
            auto.extend(words)
        
        if self.metaphone:
            words = self.get_metaphones(words)
        
        wc = {}
        for word in words:
            if word in wc:
                wc[word] += 1
            else:
                wc[word] = 1
            
        with WordItem.transaction() as t:
            linked = [link(item, word, c, transaction = t)\
                       for word,c in iteritems(wc)]
        return linked
    
    def remove_item(self, item):
        '''\
Remove indexes for *item*.

:parameter item: an instance of a :class:`stdnet.orm.StdModel`.        
'''
        if isclass(item):
            wi = WordItem.objects.filter(model_type = item)
        else:
            wi = WordItem.objects.filter(model_type = item.__class__,
                                         object_id = item.id)
        wi.delete()
    
    def words(self, text):
        '''Given a text string,
return a list of :class:`stdnet.contrib.searchengine.Word` instances
associated with it. The word items can be used to perform search
on registered models.''' 
        auto = self.autocomplete
        texts = self.get_words_from_text(text)
        if auto:
            otexts = list(texts)
            if not otexts:
                autotext = text.strip()
                texts = list(auto.search(autotext))
            else:
                autotext = otexts[-1]
                texts = otexts[:-1]
                N = len(texts)
                texts.extend(auto.search(autotext))
                if len(texts) == N:
                    texts = otexts
        if self.metaphone:
            texts = self.get_metaphones(texts)
            
        return list(Word.objects.filter(id__in = texts))
    
    def search(self, text, include = None, exclude = None):
        '''Full text search'''
        return list(self.items_from_text(text,include,exclude))
    
    def search_model(self, model, text):
        '''Return a query for ids of model instances containing
words in text.'''
        words = self.words(text)
        if not words:
            return None
        items = WordItem.objects.filter(model_type = model, word__in = words)
        return model.objects.from_query(items,'object_id')
        
    def add_tag(self, item, text):
        '''A a tag to an object.
    If the object already has the tag associated with it do nothing.
    
    :parameter item: instance of :class:`stdnet.orm.StdModel`.
    :parameter tag: a string for the tag name or a :class:`Tag` instance.
    
    It returns an instance of :class:`TaggedItem`.
    '''
        linked = []
        link = self._link_item_and_word
        for word in self.get_words_from_text(text):
            ctag = self.get_or_create(word, tag = True)
            linked.append(link(item, ctag))
        return linked
    
    def tags_for_item(self, item):
        return list(self.words_for_item(item, True))                

    def alltags(self, *models):
        '''Return a dictionary where keys are tag names and values are integers
        representing how many times the corresponding tag has been used against
        the Model classes in question.'''
        tags = {}
        for wi in WordItem.objects.filter(model_type__in = models):
            word = wi.word
            if word.tag:
                if word in tags:
                    tags[word] += 1
                else:
                    tags[word] = 1
        return tags

    # INTERNALS

    def words_for_item(self, item, tag = None):
        wis = WordItem.objects.filter(model_type = item.__class__,\
                                      object_id = item.id)
        if tag is not None:
            for wi in wis:
                if wi.word.tag == tag:
                    yield wi.word
        else:
            for wi in wis:
                yield wi.word
        
    def add_processor(self, processor):
        if processor not in self.ITEM_PROCESSORS:
            self.ITEM_PROCESSORS.append(processor)
            
    def get_words_from_text(self, text):
        #A generator of words to index from the given text. The text is
        # split by white spaces and splitters characters (if available)
        if not text:
            raise StopIteration
        
        if self.punctuation_regex:
            text = self.punctuation_regex.sub(" ", text)
        mwl = self.MIN_WORD_LENGTH
        stp = self.STOP_WORDS
        for word in text.split():
            if len(word) >= mwl:
                word = word.lower()
                if word not in stp:
                    yield word
                    
    def _link_item_and_word(self, item, word, count = 1, tag = False,
                            transaction = None):
        w = self.get_or_create(word, tag = tag)
        return WordItem(word = w,
                        model_type = item.__class__,
                        object_id = item.id,
                        count = count).save(transaction)
    
    def get_metaphones(self, words):
        """Get the metaphones for a given list of words"""
        metaphones = set()
        add = metaphones.add
        for word in words:
            metaphone = double_metaphone(to_string(word))
            w = metaphone[0].strip()
            if w:
                add(w)
            if(metaphone[1]):
                w = metaphone[1].strip()
                if w:
                    add(w)
        return metaphones
    
    def item_field_iterator(self, item):
        for processor in self.ITEM_PROCESSORS:
            result = processor(item)
            if result:
                return result
        raise ValueError(
                'Cound not iterate through item {0} fields'.format(item))
    
    def get_or_create(self, word, tag = False):
        # Internal for adding or creating words
        try:
            w = Word.objects.get(id = word)
            if tag and not w.tag:
                w.tag = True
                return w.save()
            else:
                return w
        except Word.DoesNotExist:
            return Word(id = word, tag = tag).save()
        
    def items_from_text(self, text, include = None, exclude = None):
        auto = self.autocomplete
        texts = self.get_words_from_text(text)
        if auto:
            otexts = list(texts)
            if not otexts:
                autotext = text.strip()
                texts = list(auto.search(autotext))
            else:
                autotext = otexts[-1]
                texts = otexts[:-1]
                N = len(texts)
                texts.extend(auto.search(autotext))
                if len(texts) == N:
                    texts = otexts
        words = Word.objects.filter(id__in =\
                     [m for m in self.get_metaphones(texts)])
        processed = set()
        if words:
            items = WordItem.objects.filter(word__in = words)
            if include:
                if isinstance(include,(list,tuple)):
                    items = items.filter(model_type__in = include)
                else:
                    items = items.filter(model_type = include)
            if exclude:
                if isinstance(include,(list,tuple)):
                    items = items.exclude(model_type__in = exclude)
                else:
                    items = items.exclude(model_type = exclude)
            for item in items:
                yield item.object
        else:
            raise StopIteration
        
    def reindex(self, *models):
        '''Reindex models by removing items in
:class:`stdnet.contrib.searchengine.WordItem` and rebuilding them by iterating
through all the instances of model provided.
If models are not provided, it reindex all models registered
with the search engine.'''
        if not models:
            models = self.REGISTERED_MODELS
        for model in models:
            if model in self.REGISTERED_MODELS:
                WordItem.objects.filter(model_type = model).delete()
                for obj in model.objects.all():
                    self.index_item(obj,True)
        

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
        if isinstance(item,orm.StdModel):
            return self.field_iterator(item)
    
    def field_iterator(self, item):
        for field in item._meta.fields:
            if field.type == 'text':
                value = getattr(item,field.attname)
                if value:
                    yield value


