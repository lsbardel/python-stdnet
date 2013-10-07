from stdnet import odm


class Base(odm.StdModel):
    name = odm.SymbolField(primary_key=True)
    ccy = odm.SymbolField()

    def __unicode__(self):
        return self.name

    class Meta:
        abstract = True


class Instrument(Base):
    type = odm.SymbolField()
    description = odm.CharField()
