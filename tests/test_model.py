# -*- coding: utf-8 -*-

from __future__ import absolute_import

# External Libraries
import pytest

# Project Library
from dorm import model
from dorm.db import get_db
from dorm.fields import (
    IntegerField,
    TextField,
)
from dorm.model import Q


class MyModel(model.Model):

    name = TextField()
    age = IntegerField()


@pytest.fixture
def conn():
    return get_db()


@pytest.fixture
def cursor(conn):
    return conn.cursor()

@pytest.fixture
def model1():
    return MyModel(name='adam', age=33)

@pytest.fixture
def model2():
    return MyModel(name='ann', age=33)

@pytest.fixture
def model3():
    return MyModel(name='jason', age=30)

@pytest.fixture
def model4():
    return MyModel(name='amanda', age=29)


@pytest.fixture
def some_rows(conn, cursor, model1, model2, model3, model4):
    try:
        cursor.execute(MyModel.objects.__create_sql__())
        cursor.execute(MyModel.objects.__insert_sql__(model1))
        cursor.execute(MyModel.objects.__insert_sql__(model2))
        cursor.execute(MyModel.objects.__insert_sql__(model3))
        cursor.execute(MyModel.objects.__insert_sql__(model4))
        conn.commit()
    except Exception:
        pass


def test_model():

    m = MyModel()

    for sub_sql in (
        'CREATE TABLE mymodel',
        'field_b INTEGER',
        'id INTEGER PRIMARY KEY AUTOINCREMENT',
        'field_a TEXT',
        ');'
    ):
        assert sub_sql in MyModel.objects.__create_sql__()

    assert MyModel.objects.__select_sql__(Q(id=1)).endswith('FROM mymodel WHERE id == 1;')
    assert MyModel.objects.__insert_sql__(m) == 'INSERT INTO mymodel (field_b, id, field_a) VALUES (NULL, NULL, NULL);'


def test_model(conn, cursor, some_rows, model1):

    m = MyModel.objects.get(id=1)
    assert m.age == model1.age
    assert m.name == model1.name

    m = MyModel.objects.get(name=model1.name)
    assert m.age == model1.age
    assert m.name == model1.name

def test_create(conn, cursor, model1):
    m = MyModel.objects.create(name='marley', age=9)
    assert m.id

def test_filter(conn, cursor, some_rows):

    m = [_ for _ in MyModel.objects.filter(age=33)]
    assert len(m) == 2
