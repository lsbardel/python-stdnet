
from stdnet import odm


class TaskData(odm.StdModel):
    id = odm.SymbolField(primary_key=True)
    overlap_id = odm.SymbolField(required=False)
    name = odm.SymbolField()
    status = odm.SymbolField()
    args = odm.PickleObjectField()
    kwargs = odm.PickleObjectField()
    result = odm.PickleObjectField()
    from_task = odm.SymbolField(required=False)
    time_executed = odm.DateTimeField(index=False)
    time_started = odm.DateTimeField(required=False, index=False)
    time_ended = odm.DateTimeField(required=False, index=False)
    expiry = odm.DateTimeField(required=False, index=False)
    meta = odm.JSONField()
    #
    # List where all TaskData ids are queued
    queue = odm.ListField(class_field=True)
    # Set where TaskData ids under execution are stored
    executing = odm.SetField(class_field=True)

    class Meta:
        app_label = 'tasks'

    def as_task(self):
        params = dict(self.meta or {})
        for field in self._meta.scalarfields:
            params[field.name] = getattr(self, field.attname, None)
        return backends.Task(self.id, **params)

    def __unicode__(self):
        return '%s (%s)' % (self.name, self.status)
