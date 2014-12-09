# -*- coding: utf-8 -*-

"""
The mysql pub will use the ``python-mysql-replication`` binlog stream as the
source of events.

The events pub flow::

                                                       +---------------------+
                                                       |                     |
                                                   +--->  table_write event  |
                                                   |   |                     |
                                                   |   +---------------------+
                                                   |
    +--------------------+      +---------------+  |
    |                    |      |               |  |   +---------------------+
    |        mysql       |      |   meepo.pub   |  |   |                     |
    |                    +------>               +--+--->  table_update event |
    |  row-based binlog  |      |   mysql_pub   |  |   |                     |
    |                    |      |               |  |   +---------------------+
    +--------------------+      +---------------+  |
                                                   |
                                                   |   +---------------------+
                                                   |   |                     |
                                                   +--->  table_delete event |
                                                       |                     |
                                                       +---------------------+

"""

from __future__ import absolute_import

import logging
logger = logging.getLogger("meepo.pub.mysql_pub")

import datetime
import random

from blinker import signal
import pymysqlreplication
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

from .._compat import urlparse, str


def mysql_pub(mysql_dsn, tables=None, blocking=False, **kwargs):
    """MySQL row-based binlog events pub.

    **General Usage**

    Listen and pub all tables events::

        mysql_pub(mysql_dsn)

    Listen and pub only some tables events::

        mysql_pub(mysql_dsn, tables=["test"])

    By default the ``mysql_pub`` will process and pub all existing
    row-based binlog (starting from current binlog file with pos 0) and
    quit, you may set blocking to True to block and wait for new binlog,
    enable this option if you're running the script as a daemon::

        mysql_pub(mysql_dsn, blocking=True)

    The binlog stream act as a mysql slave and read binlog from master, so the
    server_id matters, if it's conflict with other slaves or scripts, strange
    bugs may happen. By default, the server_id is randomized by
    ``randint(1000000000, 4294967295)``, you may set it to a specific value
    by server_id arg::

        mysql_pub(mysql_dsn, blocking=True, server_id=1024)

    **Signals Illustrate**

    Sometimes you want more info than the pk value, the mysql_pub expose
    a raw signal which will send the original binlog stream events.

    For example, the following sql::

        INSERT INTO test (data) VALUES ('a');

    The row-based binlog generated from the sql, reads by binlog stream and
    generates signals equals to::

        signal("test_write").send(1)
        signal("test_write_raw").send({'values': {'data': 'a', 'id': 1}})

    **Binlog Pos Signal**

    The mysql_pub has a unique signal ``mysql_binlog_pos`` which contains
    the binlog file and binlog pos, you can record the signal and resume
    binlog stream from last position with it.

    :param mysql_dsn: mysql dsn with row-based binlog enabled.
    :param tables: which tables to enable mysql_pub.
    :param blocking: whether mysql_pub should wait more binlog when all
     existing binlog processed.
    :param kwargs: more kwargs to be passed to binlog stream.
    """
    # parse mysql settings
    parsed = urlparse(mysql_dsn)
    mysql_settings = {
        "host": parsed.hostname,
        "port": parsed.port or 3306,
        "user": parsed.username,
        "passwd": parsed.password
    }

    # connect to binlog stream
    stream = pymysqlreplication.BinLogStreamReader(
        mysql_settings,
        server_id=random.randint(1000000000, 4294967295),
        blocking=blocking,
        only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent],
        **kwargs
    )

    def _pk(values):
        if isinstance(event.primary_key, str):
            return values[event.primary_key]
        return tuple(values[k] for k in event.primary_key)

    for event in stream:
        if not event.primary_key:
            continue

        if tables and event.table not in tables:
            continue

        try:
            rows = event.rows
        except (UnicodeDecodeError, ValueError) as e:
            logger.exception(e)
            continue

        timestamp = datetime.datetime.fromtimestamp(event.timestamp)

        if isinstance(event, WriteRowsEvent):
            sg_name = "{}_write".format(event.table)
            sg = signal(sg_name)
            sg_raw = signal("{}_raw".format(sg_name))

            for row in rows:
                pk = _pk(row["values"])
                sg.send(pk)
                sg_raw.send(row)

                logger.debug("%s -> %s, %s" % (sg_name, pk, timestamp))

        elif isinstance(event, UpdateRowsEvent):
            sg_name = "{}_update".format(event.table)
            sg = signal(sg_name)
            sg_raw = signal("{}_raw".format(sg_name))

            for row in rows:
                pk = _pk(row["after_values"])
                sg.send(pk)
                sg_raw.send(row)

                logger.debug("%s -> %s, %s" % (sg_name, pk, timestamp))

        elif isinstance(event, DeleteRowsEvent):
            sg_name = "{}_delete".format(event.table)
            sg = signal(sg_name)
            sg_raw = signal("{}_raw".format(sg_name))

            for row in rows:
                pk = _pk(row["values"])
                sg.send(pk)
                sg_raw.send(row)

                logger.debug("%s -> %s, %s" % (sg_name, pk, timestamp))

        signal("mysql_binlog_pos").send(
            "%s:%s" % (stream.log_file, stream.log_pos))
