from examples.models import SimpleModel
from examples.wordsearch.basicwords import basic_english_words

from stdnet.utils import test
from stdnet.utils.py2py3 import zip


class TextGenerator(test.DataGenerator):
    sizes = {
        "tiny": (20, 5),
        "small": (100, 20),
        "normal": (500, 50),
        "big": (2000, 100),
        "huge": (10000, 200),
    }

    def generate(self):
        size, words = self.size
        self.descriptions = []
        self.names = self.populate("string", size, min_len=10, max_len=30)
        for s in range(size):
            d = " ".join(
                self.populate("choice", words, choice_from=basic_english_words)
            )
            self.descriptions.append(d)


class TestFieldSerach(test.TestCase):
    model = SimpleModel
    data_cls = TextGenerator

    @classmethod
    def after_setup(cls):
        with cls.session().begin() as t:
            for name, des in zip(cls.data.names, cls.data.descriptions):
                t.add(cls.model(code=name, description=des))
        yield t.on_result

    def test_contains(self):
        session = self.session()
        qs = session.query(self.model)
        all = yield qs.filter(description__contains="ll").all()
        self.assertTrue(all)
        for m in all:
            self.assertTrue("ll" in m.description)
        all = yield qs.filter(description__contains="llllll").all()
        self.assertFalse(all)

    def test_startswith(self):
        session = self.session()
        qs = session.query(self.model)
        all = yield qs.all()
        count = {}
        for m in all:
            start = m.description.split(" ")[0][:2]
            if start in count:
                count[start] += 1
            else:
                count[start] = 1
        ordered = [k for k, _ in sorted(count.items(), key=lambda x: x[1])]
        start = ordered[-1]
        all = yield qs.filter(description__startswith=start).all()
        self.assertTrue(all)
        for m in all:
            self.assertTrue(m.description.startswith(start))
        self.assertEqual(len(all), count[start])
