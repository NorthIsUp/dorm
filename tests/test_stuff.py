import dorm
from dorm.db import get_db
from path import path

def test_noop():
    pass


def test_db_connection():
    db_path = path('/tmp/test.db')
    db_path.remove_p()
    assert not db_path.exists()

    conn = get_db(db_path)
    assert db_path.exists()

    c = conn.cursor()
    c.close()

    conn1 = get_db(db_path)
    conn2 = get_db(db_path)
    conn3 = get_db(db_path + 'foo')
    assert conn1 is conn2
    assert conn1 is not conn3

    db_path.remove_p()
    (db_path + 'foo').remove_p()

    assert not db_path.exists()
