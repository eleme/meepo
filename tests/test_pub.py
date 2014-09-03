# -*- coding: utf-8 -*-

from __future__ import absolute_import

import pymysql
from blinker import signal

from meepo._compat import urlparse


def test_mysql_pub(mysql_dsn):
    test_write_sig, test_update_sig, test_delete_sig = [], [], []

    signal("test_write").connect(
        lambda pk: test_write_sig.append(pk), weak=False)
    signal("test_update").connect(
        lambda pk: test_update_sig.append(pk), weak=False)
    signal("test_delete").connect(
        lambda pk: test_delete_sig.append(pk), weak=False)

    parsed = urlparse(mysql_dsn)
    db_settings = {
        "host": parsed.hostname,
        "port": parsed.port or 3306,
        "user": parsed.username,
        "passwd": parsed.password,
        "database": "meepo_test"
    }
    conn = pymysql.connect(**db_settings)
    cursor = conn.cursor()

    # test sqls
    sql = """
    INSERT INTO test (data) VALUES ('a');
    INSERT INTO test (data) VALUES ('b'), ('c'), ('d');
    UPDATE test SET data = 'aa' WHERE id = 1;
    UPDATE test SET data = 'bb' WHERE id = 2;
    UPDATE test SET data = 'cc' WHERE id != 1;
    DELETE FROM test WHERE id != 1;
    DELETE FROM test WHERE id = 1;
    """
    cursor.execute(sql)
    cursor.close()
    conn.commit()
    conn.close()

    from meepo.pub import mysql_pub
    mysql_pub(mysql_dsn, tables=["test"])

    assert test_write_sig == [1, 2, 3, 4]
    assert test_update_sig == [1, 2, 2, 3, 4]
    assert test_delete_sig == [2, 3, 4, 1]
