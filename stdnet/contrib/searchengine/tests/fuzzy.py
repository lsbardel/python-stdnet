from stdnet import test, getdb
from stdnet.utils import zip
from stdnet.utils.populate import populate
from stdnet.contrib.searchengine import PUNCTUATION_CHARS, Word, WordItem,\
                                        AutoComplete, SearchEngine

from stdnet.contrib.searchengine.tests.testsearch.models import Item

from .basicwords import basic_english_words


def text_gen(N):
    words = populate('choice',N,choice_from=basic_english_words)
    sp = PUNCTUATION_CHARS + '\n          '
    seps = populate('choice',N,choice_from=sp)
    for w,p in zip(words,seps):
        yield '{0}{1}'.format(w,p)
            
            
def make_text(N):
    return ''.join(text_gen(N))


def makeItems(N = 100,text_len = 100):
    names = populate('choice',N,choice_from=basic_english_words)
    nums = populate('integer',N,start=0,end=1000)
    with Item.transaction() as t:
        for name,num in zip(names,nums):
            text = make_text(text_len)
            Item(name = name, content = text, counter = num).save(t)
    
def index(engine):
    for item in Item.objects.all():
        engine.index_item(item,skipremove=True)
        

class SearchEngineTest(test.TestCase):
    
    def register(self):
        self.engine = SearchEngine(autocomplete = False)
        self.orm.register(Word)
        self.orm.register(Item)
        self.orm.register(WordItem)
        
    def testSmall(self):
        '''Make 100 items with 200 text words each'''
        makeItems(100,200)
        index(self.engine)
        #
        #Now lets do some searches
        words = populate('choice',10,choice_from=basic_english_words)
        for word in words:
            self.engine.search(word)

    
