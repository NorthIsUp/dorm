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
        """
        Helper for logging the sql strings of the __sql__ methods
        """
        @wraps(f)
        def wrapper(*args, **kwargs):
            sql = f(*args, **kwargs)
            logger.debug(sql)
            return sql
        return wrapper

    @property
    @contextmanager
    def cursor(self):
        """
        A helper to open and close database cursors

        >>> with self.cursor() as c:
        >>>    c.execute('whatever')

        """

        c = self.db.cursor()
        yield c
        c.close()

    def get(self, **kwargs):
        """
        Gets a row from the db and returns it as a model.

        Args:
            **kwargs: the fields and values to match when getting an object

        Raises:
            NoRowsReturned: if there are no models found
            MultipleRowsReturned: if more than one row is found

        Returns: an existing model

        """
        #TODO make get work
        with self.cursor as c:
             pass

    def create(self, **kwargs):
        """
        Create a model with kwargs as the field values

        Args:
            **kwargs: field names and values

        Returns: a new model

        """
        instance = self.model(**kwargs)

        with self.cursor as c:
            # todo handle db constraint exceptions
            c.execute(self.__insert_sql__(instance))
            return self.get(id=c.lastrowid)

    def get_or_create(self, **kwargs):
        """"""
        try:
            return False, self.get(**kwargs)
        except DatabaseException:
            return True, self.create(**kwargs)

    def filter(self, *Qs, **kwargs):
        """
        Returns an iterable of models given query objects

        Args:
            *Qs: Query objects to use for the filter
            **kwargs: other fields and values to use for the filter

        Returns:

        """
        #TODO: make me return models, not rows
        #TODO: write a test
        #TODO: make me lazy

        q = Q(**kwargs)  # make a query with the given kwargs
        with self.cursor as c:

            rows = c.execute(self.__select_sql__(*(q, ) + Qs))
            return rows.fetchall()

    def update(self, instance, **kwargs):
        """

        Args:
            instance: instance to save to db
            **kwargs: field names and values to save

        Returns (int): number of updated rows

        """
        #TODO: use the field serializers
        with self.cursor as c:
            rows = c.execute(self.__update_sql__(instance.id, **kwargs))
            return len(rows)

    def field_names(self):
        return self.model.fields().keys()


    @log_sql
    def __create_sql__(self):
        """
        SQL for creating the inital table

        Returns: SQL string
        """
        #TODO: use field serializers
        return 'CREATE TABLE {table_name} ({fields});'.format(
            table_name=self.table,
            fields=', '.join(field.__column_sql__() for field in self.model.fields().values()),
        )

    @log_sql
    def __select_sql__(self, *conditions):
        """
        SQL for selecting rows from the datase
        Args:
            *conditions (*Q): a list of query objects for the select statement

        Returns: SQL string

        """
        #TODO: use field serializers
        return 'SELECT {field_names} FROM {table} WHERE {conditions};'.format(
            field_names=', '.join(self.field_names()),
            table=self.table,
            conditions=' AND '.join(map(str, conditions))
        )

    @log_sql
    def __insert_sql__(self, instance):
        """

        Args:
            instance: instance to insert

        Returns: SQL string

        """

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
        """

        Args:
            id (int): id of the object to update
            **kwargs:  values for column_name and insert value

        Returns (str): SQL for updating a row

        """
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

        attrs.setdefault('id', IdField())  # ensure there is always an id column

        cls = super(ModelMetaclass, mcs).__new__(mcs, name, bases, {'__module__': module})
        cls._fields = OrderedDict()  # all the field objects

        for k, v in attrs.items():

            # set instances of field on the model
            if isinstance(v, BaseField):
                v.name = k
                v.object = cls
                cls._fields[k] = v  # field objects get saved in _fields
                setattr(cls, k, v.default)
            else:
                setattr(cls, k, v)

        # give the manager a handle to the model
        cls.objects.model = cls

        # give the manager the db connection
        cls.objects.db = cls.Meta.db

        # give the manager the table name
        cls.objects.table = cls.Meta.table or cls.__name__.lower()

        return cls


class Model(object):

    __metaclass__ = ModelMetaclass

    objects = Manager()

    class Meta:
        # set the table name, this will default to `class.__name__.lower()`
        table = None

        # database name, this can be a string or conn object
        db = get_db()

    def __init__(self, **kwargs):

        # set the values of fields that are passed in
        # iff the kwarg is actually a field
        for k, v in kwargs.items():
            if k in self._fields:
                setattr(self, k, v)

    def save(self):
        #TODO: make me work as "insert or update"
        self.objects.update(self, self.field_values())

    @classmethod
    def fields(cls):
        """
        Returns (dict): Dict of the fields on the class
        """
        fields = cls._fields.copy()
        fields.update(getattr(super(Model, cls), 'fields', lambda: {})())
        return fields

    def field_values(self):
        """
        Returns (dict): Dict of field names and values for the instance
        """

        return {field.name: getattr(self, field.name) for field in self.fields()}
