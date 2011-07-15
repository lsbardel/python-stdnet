'''Object Relational Mapper for remote data structures.'''
from .models import *
from .mapper import *
from .fields import *
from .std import *
from .signals import *
from .globals import hashmodel, JSPLITTER
from .utils import *
from .search import SearchEngine


def test_unique(fieldname, model, value, instance = None, exception = None):
    '''Test if a given fieldvalue is unique in the model table.
If not an the instance is not the same an exception is raised.'''
    try:
        r = model.objects.get(**{fieldname:value})
    except model.DoesNotExist:
        return value
    
    if instance and r.id == instance.id:
        return value
    else:
        exception = exception or model.DoesNotValidate
        raise exception('An instance with {0} {1} \
 is already available'.format(fieldname,value))