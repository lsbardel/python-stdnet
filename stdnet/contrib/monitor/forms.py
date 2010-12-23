
from djpcms import forms


class ModelFormOptions(object):
    def __init__(self, options=None):
        self.model = getattr(options, 'model', None)
        self.fields = getattr(options, 'fields', None)
        self.exclude = getattr(options, 'exclude', None)
        

class StdModelFormMetaclass(type):
    
    def __new__(cls, name, bases, attrs):
        try:
            parents = [b for b in bases if issubclass(b, StdForm)]
        except NameError:
            # We are defining ModelForm itself.
            parents = None
        declared_fields = forms.get_declared_fields(bases, attrs, False)
        new_class = super(StdModelFormMetaclass, cls).__new__(cls, name, bases, attrs)
        opts = new_class._meta = ModelFormOptions(getattr(new_class, 'Meta', None))
        if opts.model:
            # If a model is defined, extract form fields from it.
            #fields = fields_for_model(opts.model, opts.fields,
            #                          opts.exclude, opts.widgets, formfield_callback)
            # Override default model fields with any custom declared ones
            # (plus, include all the other declared fields).
            #fields.update(declared_fields)
            fields = declared_fields
        else:
            fields = declared_fields
        new_class.declared_fields = declared_fields
        new_class.base_fields = fields
        if not parents:
            return new_class
        if 'media' not in attrs:
            new_class.media = forms.media_property(new_class)
        return new_class



class StdForm(forms.BaseForm):
    __metaclass__ = StdModelFormMetaclass
    ValidationError = forms.ValidationError
    id = forms.CharField(widget=forms.HiddenInput, required = False)
    
    def __init__(self, *args, **kwargs):
        instance = kwargs.pop('instance',None)
        initial  = kwargs.pop('initial',None) or {}
        kwargs.pop('save_as_new',None)
        if not instance:
            instance = self._meta.model()
            self.adding = True
        else:
            initial.update(instance.model_to_dict())
            self.adding = False
        self.instance = instance
        self.request  = kwargs.pop('request',None)
        kwargs['initial'] = initial
        super(StdForm,self).__init__(*args, **kwargs)
        
    def save(self, commit = True):
        cd = self.cleaned_data
        instance = self.instance
        for attr,val in cd.iteritems():
            setattr(instance,attr,val)
        if commit:
            instance.save()
        return instance
    