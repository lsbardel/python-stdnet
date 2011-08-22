'''\
An implementation of :class:`stdnet.orm.SearchEngine`
based on stdnet models.

Usage
===========

Somewhere in your application create the search engine singletone::

    from stdnet.contrib.searchengine import SearchEngine
     
    engine = SearchEngine(...)
 
The engine works by registering models to it.
For example::

    engine.register(MyModel)

From now on, every time and instance of ``MyModel`` is created,
the search engine will updated its indexes.

To search, issue the command::

    search_result = engine.search(sometext)
    
If you would like to limit the search to some specified models::

    search_result = engine.search(sometext, include = (model1,model2,...))
'''
import re
from inspect import isclass

from stdnet import orm
from stdnet.utils import to_string, iteritems
from stdnet.orm.query import field_query

from .models import Word, WordItem, AutoComplete
from .ignore import STOP_WORDS, PUNCTUATION_CHARS
from .processors.metaphone import dm as double_metaphone
from .processors.porter import PorterStemmer


class stopwords:
    
    def __init__(self, stp):
        self.stp = stp
        
    def __call__(self, words):
        stp = self.stp
        for word in words:
            if word not in stp:
                yield word
        
        
def metaphone_processor(words):
    '''Double metaphone word processor'''
    for word in words:
        for w in double_metaphone(word):
            if w:
                w = w.strip()
                if w:
                    yield w
                
                
def stemming_processor(words):
    '''Double metaphone word processor'''
    stem = PorterStemmer().stem
    for word in words:
        word = stem(word, 0, len(word)-1)
        yield word


def autocomplete_processor(words):
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
    
    
class SearchEngine(orm.SearchEngine):
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
                 stemming = True, splitters = None):
        super(SearchEngine,self).__init__()
        self.MIN_WORD_LENGTH = min_word_length
        stop_words = stop_words if stop_words is not None else STOP_WORDS
        splitters = splitters if splitters is not None else PUNCTUATION_CHARS
        if splitters: 
            self.punctuation_regex = re.compile(\
                                    r"[%s]" % re.escape(splitters))
        else:
            self.punctuation_regex = None
        if stop_words:
            self.add_word_middleware(stopwords(stop_words),False)
        if stemming:
            self.add_word_middleware(stemming_processor)
        if metaphone:
            self.add_word_middleware(metaphone_processor)
        self._autocomplete = autocomplete
        
    def split_text(self, text):
        if self.punctuation_regex:
            text = self.punctuation_regex.sub(" ", text)
        mwl = self.MIN_WORD_LENGTH
        for word in text.split():
            if len(word) >= mwl:
                word = word.lower()
                yield word
    
    def flush(self, full = False):
        WordItem.flush()
        if full:
            Word.flush()
        
    @property
    def autocomplete(self):
        if self._autocomplete:
            ac = AutoComplete.me(self._autocomplete)
            #ac.minlen = self.MIN_WORD_LENGTH
            return ac
        
    def _index_item(self, item, words):    
        link = self._link_item_and_word
        with WordItem.transaction() as t:
            linked = [link(item, word, c, transaction = t)\
                       for word,c in iteritems(words)]
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
    
    def words(self, text, for_search = False):
        '''Given a text string,
return a list of :class:`stdnet.contrib.searchengine.Word` instances
associated with it. The word items can be used to perform search
on registered models.'''
        texts = self.words_from_text(text,for_search)
        if texts:
            return list(Word.objects.filter(id__in = texts))
        else:
            return None
    
    def search(self, text, include = None, exclude = None):
        '''Full text search'''
        return list(self.items_from_text(text,include,exclude))
    
    def search_model(self, model, text):
        '''Return a query for ids of model instances containing
words in text.'''
        words = self.words(text,for_search=True)
        if words is None:
            return model.objects.all()
        elif not words:
            return model.objects.empty()
        
        qs = WordItem.objects.filter(model_type = model)
        qsets = []
        for word in words:
            qsets.append(field_query(qs.filter(word = word),'object_id'))
        return model.objects.from_queries(qsets)
        
    def add_tag(self, item, text):
        '''Add a tag to an object.
    If the object already has the tag associated with it do nothing.
    
    :parameter item: instance of :class:`stdnet.orm.StdModel`.
    :parameter tag: a string for the tag name or a :class:`Tag` instance.
    
    It returns an instance of :class:`TaggedItem`.
    '''
        linked = []
        link = self._link_item_and_word
        for word in self.words_from_text(text):
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
                    
    def _link_item_and_word(self, item, word, count = 1, tag = False,
                            transaction = None):
        w = self.get_or_create(word, tag = tag)
        return WordItem(word = w,
                        model_type = item.__class__,
                        object_id = item.id,
                        count = count).save(transaction)
    
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
   



