# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
logging.basicConfig(level=logging.DEBUG)

import json
import os
import uuid

import pymysql
import pytest
import redis

from meepo._compat import urlparse


@pytest.fixture(scope="session")
def conf():
    """Try load local conf.json
    """
    fname = os.path.join(os.path.dirname(__file__), "conf.json")
    if os.path.exists(fname):
        with open(fname) as f:
            return json.load(f)


@pytest.fixture(scope="session")
def redis_dsn(request, conf):
    """Redis server dsn
    """
    redis_dsn = conf["redis_dsn"] if conf else "redis://localhost:6379/1"

    def fin():
        r = redis.Redis.from_url(redis_dsn, socket_timeout=1)
        r.flushdb()
    request.addfinalizer(fin)
    return redis_dsn


@pytest.fixture(scope="module")
def mysql_dsn(conf):
    """MySQL server dsn

    This fixture will init a clean meepo_test database with a 'test' table
    """
    logger = logging.getLogger("fixture_mysql_dsn")

    dsn = conf["mysql_dsn"] if conf else \
        "mysql+pymysql://root@localhost/meepo_test"

    # init database
    parsed = urlparse(dsn)
    db_settings = {
        "host": parsed.hostname,
        "port": parsed.port or 3306,
        "user": parsed.username,
        "passwd": parsed.password
    }
    conn = pymysql.connect(**db_settings)

    cursor = conn.cursor()
    sql = """
    DROP DATABASE IF EXISTS meepo_test;
    CREATE DATABASE meepo_test;
    DROP TABLE IF EXISTS meepo_test.test;
    CREATE TABLE meepo_test.test (
        id INT NOT NULL AUTO_INCREMENT,
        data VARCHAR (256) NOT NULL,
        PRIMARY KEY (id)
    );
    RESET MASTER;
    """
    cursor.execute(sql)
    logger.debug("executed")

    # release conn
    cursor.close()
    conn.close()

    return dsn


@pytest.fixture(scope="module")
def mock_session():
    class MockSession(object):
        def __init__(self):
            self.meepo_unique_id = uuid.uuid4().hex
    return MockSession()
