import logging
import json
import sys
import csv
from inspect import isclass

from stdnet.utils import StringIO

from .base import get_model_from_hash

__all__ = ['get_serializer',
           'register_serializer',
           'unregister_serializer',
           'all_serializers',
           'Serializer']


logger = logging.getLogger('stdnet')

_serializers = {}


if sys.version_info < (2,7):    # pragma: no cover
    def writeheader(dw):
        # hack to handle writeheader in python 2.6
        dw.writerow(dict(((k,k) for k in dw.fieldnames)))
else:
    def writeheader(dw):
        dw.writeheader()


def get_serializer(name, **options):
    '''Retrieve a serializer register as *name*. If the serializer is not
available an exception will raise. A common use usage pattern::

    qs = MyModel.objects.query().sort_by('id')
    s = odm.get_serializer('json')
    s.serialize(qs)

'''
    if name in _serializers:
        serializer = _serializers[name]
        return serializer(**options)
    else:
        raise ValueError('Unknown serializer {0}.'.format(name))

def register_serializer(name, serializer):
    '''\
Register a new serializer to the library.

:parameter name: serializer name (it can override existing serializers).
:parameter serializer: an instance or a derived class of a
    :class:`stdnet.odm.Serializer` class or a callable.
'''
    if not isclass(serializer):
        serializer = serializer.__class__
    _serializers[name] = serializer

def unregister_serializer(name):
    return _serializers.pop(name,None)

def all_serializers():
    return sorted(_serializers)


class Serializer(object):
    '''The stdnet serializer base class.'''
    default_options = {}
    arguments = ()

    def __init__(self, **options):
        opts = self.default_options.copy()
        opts.update(((v,options[v]) for v in options if v in self.arguments))
        self.options = opts

    @property
    def data(self):
        if not hasattr(self,'_data'):
            self._data = []
        return self._data

    def serialize(self, qs):
        '''Serialize a :class:`Query` *qs*.'''
        raise NotImplementedError()

    def write(self, stream = None):
        raise NotImplementedError()

    def load(self, stream, model = None):
        '''Load a stream of data into the database.'''
        raise NotImplementedError()


class JsonSerializer(Serializer):
    arguments = ('indent',)

    def get_data(self, qs):
        data = []
        for obj in qs:
            data.append(obj.tojson())
            meta = obj._meta
        return {'model':str(meta),
                'hash':meta.hash,
                'data':data}

    def serialize(self, qs):
        self.data.append(self.get_data(qs))

    def write(self, stream = None):
        stream = stream or StringIO()
        line = json.dumps(self.data, stream, **self.options)
        stream.write(line)
        return stream

    def load(self, stream, model=None):
        if hasattr(stream, 'read'):
            stream = stream.read()
        data = json.loads(stream, **self.options)
        for model_data in data:
            model = get_model_from_hash(model_data['hash'])
            if model:
                logger.info('Loading model %s' % model._meta)
                with model.objects.transaction(signal_commit=False,
                                               signal_session=False) as t:
                    for item_data in model_data['data']:
                        t.add(model.from_base64_data(**item_data))
            else:
                logger.error('Could not load model %s'\
                              % model_data.get('model'))


class CsvSerializer(Serializer):
    '''A csv serializer for single model. It serialize a model query into
a csv file.'''
    default_options = {'lineterminator': '\n'}

    def serialize(self, qs):
        if self.data:
            raise ValueError('Cannot serialize more than one model into CSV')
        fields = None
        data = []
        for obj in qs:
            js = obj.tojson()
            if fields is None:
                fields = set(js)
            else:
                fields.update(js)
            data.append(js)
            meta = obj._meta
        ordered_fields = [meta.pkname()]
        ordered_fields.extend((f.name for f in meta.scalarfields\
                                if f.name in fields))
        data = {'fieldnames': ordered_fields,
                'hash': meta.hash,
                'data': data}
        self.data.append(data)

    def write(self, stream = None):
        stream = stream or StringIO()
        if self.data:
            fieldnames = self.data[0]['fieldnames']
            data = self.data[0]['data']
            if data:
                w = csv.DictWriter(stream, fieldnames, **self.options)
                writeheader(w)
                for row in data:
                    w.writerow(row)
        return stream

    def load(self, stream, model = None):
        if not model:
            raise ValueError('Model is required when loading from csv file')
        r = csv.DictReader(stream, **self.options)
        with model.objects.transaction() as t:
            for item_data in r:
                t.add(model.from_base64_data(**item_data))


register_serializer('json', JsonSerializer)
register_serializer('csv', CsvSerializer)
