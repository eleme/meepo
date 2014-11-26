# -*- coding: utf-8 -*-

"""
meepo_examples.tutorial.mysql
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A demo script on how to use meepo with mysql row-based binlog.
"""

import logging

import click
import pymysql

from meepo.logutils import setup_logger
setup_logger()
logger = logging.getLogger("meepo_examples.tutorial.mysql")

from meepo._compat import urlparse


def db_prepare(dsn):
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
    logger.info("table created.")

    # genereate binlog
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
    logger.info("binlog created.")


@click.command()
@click.option('-m', '--mysql_dsn')
def main(mysql_dsn):
    # make sure the user has permission to read binlog
    mysql_dsn = mysql_dsn or "mysql+pymysql://root@localhost/meepo_test"

    from meepo.sub import print_sub
    print_sub(["test"])

    from meepo.pub import mysql_pub
    mysql_pub(mysql_dsn, ["test"])


if __name__ == "__main__":
    main()
