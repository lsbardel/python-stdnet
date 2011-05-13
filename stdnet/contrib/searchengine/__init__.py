'''\
A stdnet application for building a search-engine on
models with ideas from the fast, fuzzy, full-text index with Redis
`blog post <http://playnice.ly/blog/2010/05/05/a-fast-fuzzy-full-text-index-using-redis/>`_.
'''
import re
from itertools import chain

from stdnet import orm
from stdnet.utils import to_string

from .models import Word, WordItem
from .ignore import STOP_WORDS, PUNCTUATION_CHARS, MIN_WORD_LENGTH
from .metaphone import dm as double_metaphone


class FullTextIndex(object):
    """A class to provide full-text indexing functionality using StdNet. Adapted from
    
https://gist.github.com/389875
"""
    ITEM_PROCESSORS = []
    
    def __init__(self, min_word_length = None, stop_words = None):
        self.MIN_WORD_LENGTH = min_word_length if min_word_length is not None else MIN_WORD_LENGTH
        self.STOP_WORDS = stop_words if stop_words is not None else STOP_WORDS
        self.punctuation_regex = re.compile(r"[%s]" % re.escape(PUNCTUATION_CHARS))
        
    def index_item(self, item):
        """Extract content from the given item and add it to the index"""
        w = self.get_words_from_text
        link = self._link_item_and_word
        
        words = chain(*[w(value) for value in self.item_field_iterator(item)])
        linked = []        
        for metaphone in self.get_metaphones(words):
            wi = link(item, metaphone)
            if wi:
                linked.append(wi)
        return linked
    
    def search(self, text, **filters):
        '''Full text search'''
        return set(self.items_from_text(text,**filters))
    
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
        """A generator of words to index from the given text"""
        if not text:
            raise StopIteration
        
        text = self.punctuation_regex.sub(" ", text)
        mwl = self.MIN_WORD_LENGTH
        stp = self.STOP_WORDS
        for word in text.split():
            if len(word) >= mwl:
                word = word.lower()
                if word not in stp:
                    yield word
                    
    def _link_item_and_word(self, item, word, tag = False):
        w = self.get_or_create(word, tag = tag)
        if not WordItem.objects.filter(word = w,
                                       model_type = item.__class__,
                                       object_id = item.id):
            return WordItem(word = w,
                            model_type = item.__class__,
                            object_id = item.id).save()
    
    def get_metaphones(self, words):
        """Get the metaphones for a given list of words"""
        metaphones = set()
        for word in words:
            metaphone = double_metaphone(to_string(word))
            metaphones.add(metaphone[0].strip())
            if(metaphone[1]):
                metaphones.add(metaphone[1].strip())
        return metaphones
    
    def item_field_iterator(self, item):
        for processor in self.ITEM_PROCESSORS:
            result = processor(item)
            if result:
                return result
        raise ValueError('Cound not iterate through item {0} fields'.format(item))
    
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
        
        
    def items_from_text(self, text, **filters):
        texts = self.get_words_from_text(text)
        words = Word.objects.filter(id__in =\
                     [m for m in self.get_metaphones(texts)])
        processed = set()
        if words:
            items = WordItem.objects.filter(word__in = words)
            if filters:
                items.filter(**filters)
            for item in items:
                yield item.object
        else:
            raise StopIteration
        

engine = FullTextIndex()


class stdnet_processor(object):
    
    def __call__(self, item):
        if isinstance(item,orm.StdModel):
            return self.field_iterator(item)
    
    def field_iterator(self, item):
        for field in item._meta.fields:
            if isinstance(field,orm.SymbolField):
                yield getattr(item,field.attname)


engine.add_processor(stdnet_processor())

