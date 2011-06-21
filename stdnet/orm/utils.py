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
    
    def serialize(self, qs, stream = None, **options):
        stream = stream or StringIO()
        data = self.get_data(qs)
        self.end_serialize(data, stream, **options)
    
    def get_data(self, qs):
        objs = []
        for obj in qs:
            data = obj.todict()
            data['id'] = obj.id
            objs.append(data)
        return objs
    
    def end_serialize(self, data, stream, **options):
        raise NotImplementedError
    
    
class JsonSerializer(Serializer):
            
    def end_serialize(self, data, stream, **options):
        json.dumps(data,stream,**options)
        
        
class CsvSerializer(Serializer):
            
    def end_serialize(self, data, stream, **options):
        w = DictWriter(stream,**options)
        for row in data:
            w.write(row)
            

_serializers['json'] = JsonSerializer()
_serializers['csv'] = CsvSerializer()
