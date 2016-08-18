# -*- coding: utf-8 -*-

from __future__ import absolute_import

# Standard Library
import sqlite3

# External Libraries
from path import path


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def get_db(name=':memory:', databases={}, row_factory=dict_factory):

    if isinstance(name, sqlite3.Connection):
        return name

    if name != ':memory:':
        name = path(name).abspath()

        if not name.dirname().exists():
            raise Exception('Can\'t create db as path doesn\'t exit')

    conn = databases.get(name) or sqlite3.connect(name)
    conn.row_factory = row_factory
    databases[name] = conn
    return conn
