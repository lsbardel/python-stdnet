import logging
import json
import sys
import csv
from inspect import isclass

from stdnet.utils import StringIO

from .globals import get_model_from_hash

__all__ = ['get_serializer',
           'register_serializer',
           'unregister_serializer',
           'all_serializers',
           'Serializer',
           'JsonSerializer']


LOGGER = logging.getLogger('stdnet.odm')

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
available a ``ValueError`` exception will raise.
A common usage pattern::
    
    qs = MyModel.objects.query().sort_by('id')
    s = odm.get_serializer('json')
    s.dump(qs)
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
    '''The stdnet serializer base class. During initialization, the *options*
dictionary is used to override the :attr:`default_options`. These are specific
to each :class:`Serializer` implementation.
    
.. attribute:: default_options

    Dictionary of default options which are overwritten during initialisation.
    By default it is an empty dictionary.
    
.. attribute:: options

    Dictionary of options.
'''
    default_options = {}
    arguments = ()

    def __init__(self, **options):
        opts = self.default_options.copy()
        opts.update(((v,options[v]) for v in options if v in self.arguments))
        self.options = opts

    @property
    def data(self):
        '''CList of data to dump into a stream.'''
        if not hasattr(self, '_data'):
            self._data = []
        return self._data

    def dump(self, qs):
        '''Add a :class:`Query` ``qs`` into the collection of :attr:`data`
to dump into a stream. No writing is done until the :meth:`write` method.'''
        raise NotImplementedError

    def write(self, stream=None):
        '''Write the serialized data into a stream. If *stream* is not
provided, a python ``StringIO`` is used.
        
:return: the stream object.'''
        raise NotImplementedError

    def load(self, models, stream, model=None):
        '''Load a stream of data into the database.

:param models: the :class:`Router` which must contains all the model this
    method will load.
:param stream: bytes or an object with a ``read`` method returning bytes.
:param model: Optional :class:`StdModel` we need to load. If not provided all
    models in ``stream`` are loaded.
    
This method must be implemented by subclasses.
'''
        raise NotImplementedError


class JsonSerializer(Serializer):
    '''The default :class:`Serializer` of :mod:`stdnet`. It
serialise/unserialise models into json data. It has one option given
by the *indent* of the ``json`` string for pretty serialisation.'''
    arguments = ('indent',)

    def get_data(self, qs):
        data = []
        for obj in qs:
            data.append(obj.tojson())
            meta = obj._meta
        if data:
            return {'model': str(meta),
                    'hash': meta.hash,
                    'data': data}

    def dump(self, qs):
        data = self.get_data(qs)
        if data:
            self.data.append(data)

    def write(self, stream=None):
        stream = stream or StringIO()
        line = json.dumps(self.data, stream, **self.options)
        stream.write(line)
        return stream

    def load(self, models, stream, model=None):
        if hasattr(stream, 'read'):
            stream = stream.read()
        data = json.loads(stream, **self.options)
        for model_data in data:
            model = get_model_from_hash(model_data['hash'])
            if model:
                model = self.on_load_model(model, model_data)
                if model:
                    manager = models[model]
                    LOGGER.info('Loading model %s', model._meta)
                    session = manager.session()
                    with session.begin(signal_commit=False) as t:
                        for item_data in model_data['data']:
                            t.add(model.from_base64_data(**item_data))
            else:
                LOGGER.error('Could not load model %s', model_data.get('model'))
        self.on_finished_load()

    def on_load_model(self, model, model_data):
        '''Callback when a *model* is about to be loaded. If it returns the
model, the model will get loaded otherwise it will skip the loading.'''
        return model
    
    def on_finished_load(self):
        '''Callback when loading of data is finished'''
        pass
    

class CsvSerializer(Serializer):
    '''A csv serializer for single model. It serialize/unserialize a model
query into a csv file.'''
    default_options = {'lineterminator': '\n'}

    def dump(self, qs):
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

    def write(self, stream=None):
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

    def load(self, models, stream, model=None):
        if not model:
            raise ValueError('Model is required when loading from csv file')
        r = csv.DictReader(stream, **self.options)
        with models.session().begin() as t:
            for item_data in r:
                t.add(model.from_base64_data(**item_data))
        return t.on_result


register_serializer('json', JsonSerializer)
register_serializer('csv', CsvSerializer)
