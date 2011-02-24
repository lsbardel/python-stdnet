from stdnet import orm


class User(orm.StdModel):
    username = orm.SymbolField(unique = True)
    password = orm.CharField(required = True)


class Issue(orm.StdModel):
    user = orm.ForeignKey(User)
    description = orm.CharField(required = True)
    body = orm.CharField()
    
    def __str__(self):
        return self.description
