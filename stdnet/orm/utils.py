import json
from csv import DictWriter
from inspect import isclass

__all__ = ['get_serializer',
           'register_serializer',
           'Serializer']


_serializers = {}


def get_serializer(name):
    '''Retrieve a serializer register as *name*. If the serializer is not
available an exception will raise. A common use usage pattern::

    qs = MyModel.objects.all().order_by('id')
    s = orm.get_serializer('json')
    s.serialize(qs)

'''
    if name in _serializers:
        return _serializers[name]
    else:
        raise ValueError('Unknown serializer {0}.'.format(name))
    
    
def register_serializer(name, serializer):
    '''\
Register a new serializer to the library.

:parameter name: serializer name (it can override existing serializers).
:parameter serializer: an instance or a derived class of a :class:`stdnet.orm.Serializer`
                       class or a callable.
'''
    if isclass(serializer):
        serializer = serializer()
    _serializers[name] = serializer
    
    
class Serializer(object):
    '''The stdnet serializer base class.'''
    
    @property
    def data(self):
        if not hasattr(self,'_data'):
            self._data = []
        return self._data
    
    def serialize(self, qs, **options):
        data = self.data.append(self.get_data(qs))
    
    def get_data(self, qs):
        data = []
        for obj in qs:
            data.append(obj.tojson())
            model = str(obj._meta)
        return {'model':model,
                'data':data}
    
    def write(self, stream, **options):
        raise NotImplementedError
    
    
class JsonSerializer(Serializer):
            
    def write(self, stream = None, **options):
        stream = stream or StringIO()
        line = json.dumps(self.data,stream,**options)
        stream.write(line)
        return stream
        
        
class CsvSerializer(Serializer):
            
    def write(self, stream = None, **options):
        stream = stream or StringIO()
        if self.data:
            if len(self.data) > 1:
                print('Cannot serialize more than one model into CSV')
                return stream
            data = self.data[0]['data']
            if data:
                fields = list(data[0])
                w = DictWriter(stream,fields,**options)
                w.writeheader()
                for row in data:
                    w.writerow(row)
        return stream
            

_serializers['json'] = JsonSerializer()
_serializers['csv'] = CsvSerializer()
