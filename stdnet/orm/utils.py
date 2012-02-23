import json
from csv import DictWriter
from inspect import isclass

from stdnet.utils import StringIO

__all__ = ['get_serializer',
           'register_serializer',
           'Serializer']


_serializers = {}


def get_serializer(name, **options):
    '''Retrieve a serializer register as *name*. If the serializer is not
available an exception will raise. A common use usage pattern::

    qs = MyModel.objects.query().sort_by('id')
    s = orm.get_serializer('json')
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
:parameter serializer: an instance or a derived class of a :class:`stdnet.orm.Serializer`
                       class or a callable.
'''
    if not isclass(serializer):
        serializer = serializer.__class__
    _serializers[name] = serializer
    
    
class Serializer(object):
    '''The stdnet serializer base class.'''
    
    def __init__(self, **options):
        self.options = options
        
    @property
    def data(self):
        if not hasattr(self,'_data'):
            self._data = []
        return self._data
    
    def serialize(self, qs):
        data = self.data.append(self.get_data(qs))
    
    def get_data(self, qs):
        data = []
        for obj in qs:
            data.append(obj.tojson())
            meta = obj._meta            
        return {'model':str(meta),
                'hash':meta.hash,
                'data':data}
    
    def write(self, stream = None):
        raise NotImplementedError()
    
    def load(self, stream):
        raise NotImplementedError()
    
    
class JsonSerializer(Serializer):
            
    def write(self, stream = None):
        stream = stream or StringIO()
        line = json.dumps(self.data, stream, **self.options)
        stream.write(line)
        return stream

    def load(self, stream):
        data = json.loads(stream, **self.options)
        for model_data in data:
            model = data['hash']
            with model.objects.transaction() as t:
                for item_data in data['data']:
                    t.add(model.from_base64_data(**item_data))
            
        
class CsvSerializer(Serializer):
            
    def write(self, stream = None):
        stream = stream or StringIO()
        if self.data:
            if len(self.data) > 1:
                print('Cannot serialize more than one model into CSV')
                return stream
            data = self.data[0]['data']
            if data:
                fields = list(data[0])
                w = DictWriter(stream,fields, **self.options)
                w.writeheader()
                for row in data:
                    w.writerow(row)
        return stream
            

register_serializer('json', JsonSerializer)
register_serializer('csv', CsvSerializer)
