import stdnet


class RelatedObject(object):
    
    def __init__(self,
                 model,
                 related_name = None,
                 relmanager = None):
        if not model:
            raise stdnet.FieldError('Model not specified')
        self.model        = model
        self.related_name = related_name
        self.relmanager   = relmanager
    
    def register_with_related_model(self, name, related):
        model = self.model
        if not model:
            return
        if model == 'self':
            model = related
        if isinstance(model,basestring):
            raise NotImplementedError
        self.model = model
        meta  = model._meta
        related_name = self.related_name or '%s_set' % related._meta.name
        if related_name not in meta.related and related_name not in meta.fields:
            self.related_name = related_name
            manager = self.relmanager(related,name)
            meta.related[related_name] = manager
            return manager
        else:
            raise stdnet.FieldError("Duplicated related name %s in model %s and field %s" % (related_name,related,name))
