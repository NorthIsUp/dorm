# -*- coding: utf-8 -*-

from __future__ import absolute_import

# Standard Library
from collections import OrderedDict
from contextlib import contextmanager
from functools import wraps
from logging import getLogger

# Project Library
from dorm.db import get_db
from dorm.fields import (
    BaseField,
    IdField,
)

logger = getLogger(__name__)

class DatabaseException(Exception):
    pass

class NoRowsReturned(DatabaseException):
    pass

class MultipleRowsReturned(DatabaseException):
    pass


class Q(object):

    def __init__(self, **fields):
        self.fields = fields

    def __str__(self):
        return ' OR '.join('{} == \'{}\''.format(k, v) for k, v in self.fields.items())


class Manager(object):

    def __init__(self, db='default.db', table=None, model=None):
        self.model = model
        self.db = get_db(db)
        self.table = table

    def log_sql(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            sql = f(*args, **kwargs)
            logger.debug(sql)
            return sql
        return wrapper

    @property
    @contextmanager
    def cursor(self):
        c = self.db.cursor()
        yield c
        c.close()

    def get(self, **kwargs):
        with self.cursor as c:
            rows = c.execute(self.__select_sql__(Q(**kwargs)))

            if rows.rowcount == 0:
                raise NoRowsReturned()
            elif rows.rowcount > 1:
                raise MultipleRowsReturned()

            return self.model(**rows.fetchone())

    def create(self, **kwargs):
        instance = self.model(**kwargs)

        with self.cursor as c:
            # todo handle db constraint exceptions
            c.execute(self.__insert_sql__(instance))
            return self.get(id=c.lastrowid)

    def get_or_create(self, **kwargs):
        try:
            return False, self.get(**kwargs)
        except DatabaseException:
            return True, self.create(**kwargs)

    def filter(self, *Qs, **kwargs):
        q = Q(**kwargs)
        with self.cursor as c:

            rows = c.execute(self.__select_sql__(*(q, ) + Qs))
            row = rows.fetchone()
            while row:
                yield row
                row = rows.fetchone()

    def update(self, instance, **kwargs):
        with self.cursor as c:
            rows = c.execute(self.__update_sql__(instance.id, **kwargs))
            return len(rows)

    def field_names(self):
        return self.model.fields().keys()


    @log_sql
    def __create_sql__(self):
        return 'CREATE TABLE {table_name} ({fields});'.format(
            table_name=self.table,
            fields=', '.join(field.__column_sql__() for field in self.model.fields().values()),
        )

    @log_sql
    def __select_sql__(self, *conditions):
        return 'SELECT {field_names} FROM {table} WHERE {conditions};'.format(
            field_names=', '.join(self.field_names()),
            table=self.table,
            conditions=' AND '.join(map(str, conditions))
        )

    @log_sql
    def __insert_sql__(self, instance):

        fields, values = zip(*{
            name: field.serialize(getattr(instance, name))
            for name, field in self.model.fields().items()
        }.items())

        return 'INSERT INTO {table} ({fields}) VALUES ({values});'.format(
            table=self.table,
            fields=', '.join(fields),
            values=', '.join(values),
        )

    @log_sql
    def __update_sql__(self, id, **kwargs):

        return 'UPDATE {table} SET {values} WHERE id = {id};'.format(
            table=self.table,
            values=', '.join(
                '{} = {}'.format(field, value) for field, value in kwargs.items()
            ),
            id=id,
        )

class ModelMetaclass(type):

    def __new__(mcs, name, bases, attrs):
        # mcs == metaclass
        # cls == new class
        module = attrs.pop('__module__')
        attrs.setdefault('id', IdField())
        cls = super(ModelMetaclass, mcs).__new__(mcs, name, bases, {'__module__': module})
        cls._fields = OrderedDict()
        cls._required = []

        for k, v in attrs.items():
            if isinstance(v, BaseField):
                v.name = k
                v.object = cls
                cls._fields[k] = v
                if v.required:
                    cls._required.append(v.name)
                setattr(cls, k, v.default)
            else:
                setattr(cls, k, v)

        cls.objects.model = cls
        cls.objects.db = cls.Meta.db
        cls.objects.table = cls.Meta.table or cls.__name__.lower()

        return cls


class Model(object):

    __metaclass__ = ModelMetaclass

    objects = Manager()

    class Meta:
        table = None
        db = get_db()

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            if k in self._fields:
                setattr(self, k, v)

    def save(self):
        self.objects.update(self, self.field_values())

    @classmethod
    def fields(cls):
        fields = cls._fields.copy()
        fields.update(getattr(super(Model, cls), 'fields', lambda: {})())
        return fields

    def field_values(self):
        return {field.name: getattr(self, field.name) for field in self.fields()}
