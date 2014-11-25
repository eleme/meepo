# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
logging.basicConfig(level=logging.DEBUG)

import pymysql
from blinker import signal

from meepo._compat import urlparse


def test_mysql_pub(mysql_dsn):
    logger = logging.getLogger("test_mysql_pub")

    def test_sg(sg_list):
        return lambda pk: sg_list.append(pk)

    # connect table action signal
    test_write_l, test_update_l, test_delete_l = [], [], []
    signal("test_write").connect(test_sg(test_write_l), weak=False)
    signal("test_update").connect(test_sg(test_update_l), weak=False)
    signal("test_delete").connect(test_sg(test_delete_l), weak=False)

    # connect raw table action signal
    test_write_raw_l, test_update_raw_l, test_delete_raw_l = [], [], []
    signal("test_write_raw").connect(test_sg(test_write_raw_l), weak=False)
    signal("test_update_raw").connect(test_sg(test_update_raw_l), weak=False)
    signal("test_delete_raw").connect(test_sg(test_delete_raw_l), weak=False)

    # connect mysql binlog pos signal
    test_binlog_l = []
    signal("mysql_binlog_pos").connect(test_sg(test_binlog_l),  weak=False)

    # init mysql connection
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
    logger.debug("mysql binlog generated")

    from meepo.pub import mysql_pub
    mysql_pub(mysql_dsn, tables=["test"])

    # test pk signal
    assert test_write_l == [1, 2, 3, 4]
    assert test_update_l == [1, 2, 2, 3, 4]
    assert test_delete_l == [2, 3, 4, 1]
    assert test_binlog_l == ['mysql-bin.000001:270', 'mysql-bin.000001:375',
                             'mysql-bin.000001:474', 'mysql-bin.000001:573',
                             'mysql-bin.000001:707', 'mysql-bin.000001:815',
                             'mysql-bin.000001:905']

    # test raw data signal
    assert test_write_raw_l == [
        {'values': {'data': 'a', 'id': 1}},
        {'values': {'data': 'b', 'id': 2}},
        {'values': {'data': 'c', 'id': 3}},
        {'values': {'data': 'd', 'id': 4}},
    ]
    assert test_update_raw_l == [
        {'before_values': {'data': 'a', 'id': 1},
         'after_values': {'data': 'aa', 'id': 1}},
        {'before_values': {'data': 'b', 'id': 2},
         'after_values': {'data': 'bb', 'id': 2}},
        {'before_values': {'data': 'bb', 'id': 2},
         'after_values': {'data': 'cc', 'id': 2}},
        {'before_values': {'data': 'c', 'id': 3},
         'after_values': {'data': 'cc', 'id': 3}},
        {'before_values': {'data': 'd', 'id': 4},
         'after_values': {'data': 'cc', 'id': 4}},
    ]
    assert test_delete_raw_l == [
        {'values': {'data': 'cc', 'id': 2}},
        {'values': {'data': 'cc', 'id': 3}},
        {'values': {'data': 'cc', 'id': 4}},
        {'values': {'data': 'aa', 'id': 1}}
    ]
