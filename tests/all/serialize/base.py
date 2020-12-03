import os
import tempfile

from stdnet import odm
from stdnet.utils import BytesIO, test, to_bytes


class Tempfile(object):
    def __init__(self, data, text=True):
        fd, path = tempfile.mkstemp(text=text)
        self.handler = None
        self.path = path
        os.write(fd, to_bytes(data))
        os.close(fd)

    def __enter__(self):
        return self

    def write(self, data):
        if self.fd:
            os.write(self.fd, data)
            os.close(self.fd)
            self.fd = None

    def close(self):
        if self.handler:
            self.handler.close()
            self.handler = None

    def open(self):
        if self.handler:
            raise RuntimeError("File is already opened")
        self.handler = open(self.path, "r")
        return self.handler

    def __exit__(self, type, value, trace):
        self.close()
        os.remove(self.path)


class BaseSerializerMixin(object):
    serializer = "json"

    @classmethod
    def after_setup(cls):
        yield cls.data.create(cls)

    def get(self, **options):
        s = odm.get_serializer(self.serializer)
        if not s.default_options:
            self.assertEqual(s.options, options)
        self.assertFalse(s.data)
        self.assertTrue(s)
        return s

    def dump(self):
        models = self.mapper
        s = self.get()
        qs = yield models.instrument.query().sort_by("id").all()
        s.dump(qs)
        self.assertTrue(s.data)
        self.assertEqual(len(s.data), 1)
        yield s


class SerializerMixin(BaseSerializerMixin):
    def testMeta(self):
        self.get()

    def test_dump(self):
        return self.dump()

    def test_write(self):
        s = yield self.dump()
        data = s.write()
        self.assertTrue(data)


class LoadSerializerMixin(BaseSerializerMixin):
    def testLoad(self):
        models = self.mapper
        s = yield self.dump()
        qs = yield models.instrument.query().sort_by("id").all()
        self.assertTrue(qs)
        data = s.write().getvalue()
        with Tempfile(data) as tmp:
            yield models.instrument.flush()
            yield s.load(models, tmp.open(), self.model)
        qs2 = yield models.instrument.query().sort_by("id").all()
        self.assertEqual(qs, qs2)


class DummySerializer(odm.Serializer):
    """A Serializer for testing registration"""

    pass


class TestMeta(test.TestCase):
    def testBadSerializer(self):
        self.assertRaises(ValueError, odm.get_serializer, "djsbvjchvsdjcvsdj")

    def testRegisterUnregister(self):
        odm.register_serializer("dummy", DummySerializer())
        s = odm.get_serializer("dummy")
        self.assertTrue("dummy" in odm.all_serializers())
        self.assertTrue(isinstance(s, DummySerializer))
        self.assertRaises(NotImplementedError, s.dump, None)
        self.assertRaises(NotImplementedError, s.write)
        self.assertRaises(NotImplementedError, s.load, None, None)
        self.assertTrue(odm.unregister_serializer("dummy"))
        self.assertRaises(ValueError, odm.get_serializer, "dummy")
