from __future__ import absolute_import


class ValidationError(Exception):
    pass


class BaseField(object):

    type = None

    def __init__(self, default=None, pk=False, **kwargs):
        self.default = default
        self.name = None
        self.object = None
        self.pk = pk

        if hasattr(self.type, 'deserialize'):
            self.deserialize = self.type.deserialize

    def deserialize(self, raw_data):
        try:
            v = self.type(raw_data)
        except (ValueError, TypeError):
            raise ValidationError('{}.{} should be convertible to {}, got {}'.format(
                self.object,
                self.name,
                self.type,
                type(raw_data)
            ))

        return v

    def serialize(self, raw_data):
        if raw_data is None:
            return 'NULL'

    def __column_sql__(self):
        column_type = self.column_type
        return '{name} {type}'.format(type=column_type, name=self.name)


class TextField(BaseField):
    type = str
    column_type = 'TEXT'

    def serialize(self, raw_data):
        return super(TextField, self).serialize(raw_data) or "'{}'".format(raw_data)


class IntegerField(BaseField):
    type = int
    column_type = 'INTEGER'

    def serialize(self, raw_data):
        return super(IntegerField, self).serialize(raw_data) or '{}'.format(raw_data)


class IdField(IntegerField):
    column_type = 'INTEGER PRIMARY KEY AUTOINCREMENT'

    def __init__(self, **kwargs):
        self.pk = kwargs.pop('pk', True)
        super(IdField, self).__init__(**kwargs)
