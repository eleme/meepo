# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
logging.basicConfig(level=logging.DEBUG)

import pymysql
import pytest

from meepo._compat import urlparse
from meepo.pub import mysql_pub
from meepo.signals import signal

t_writes, t_updates, t_deletes, t_binlogs = [], [], [], []
t_raw_writes, t_raw_updates, t_raw_deletes = [], [], []


def setup_module(module):
    def test_sg(sg_list):
        return lambda pk: sg_list.append(pk)

    # connect table action signal
    signal("test_write").connect(test_sg(t_writes), weak=False)
    signal("test_update").connect(test_sg(t_updates), weak=False)
    signal("test_delete").connect(test_sg(t_deletes), weak=False)

    # connect raw table action signal
    signal("test_write_raw").connect(test_sg(t_raw_writes), weak=False)
    signal("test_update_raw").connect(test_sg(t_raw_updates), weak=False)
    signal("test_delete_raw").connect(test_sg(t_raw_deletes), weak=False)

    # connect mysql binlog pos signal
    signal("mysql_binlog_pos").connect(test_sg(t_binlogs),  weak=False)


@pytest.fixture(scope="module")
def binlog(mysql_dsn):
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

    # generates signals
    mysql_pub(mysql_dsn, tables=["test"])


def test_mysql_table_event(binlog):
    assert t_writes == [1, 2, 3, 4]
    assert t_updates == [1, 2, 2, 3, 4]
    assert t_deletes == [2, 3, 4, 1]


def test_mysql_binlog_pos_event(binlog):
    assert all(pos.startswith("mysql-bin.000001") for pos in t_binlogs)


def test_mysql_raw_table_event(binlog):
    assert t_raw_writes == [
        {'values': {'data': 'a', 'id': 1}},
        {'values': {'data': 'b', 'id': 2}},
        {'values': {'data': 'c', 'id': 3}},
        {'values': {'data': 'd', 'id': 4}},
    ]
    assert t_raw_updates == [
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
    assert t_raw_deletes == [
        {'values': {'data': 'cc', 'id': 2}},
        {'values': {'data': 'cc', 'id': 3}},
        {'values': {'data': 'cc', 'id': 4}},
        {'values': {'data': 'aa', 'id': 1}}
    ]
