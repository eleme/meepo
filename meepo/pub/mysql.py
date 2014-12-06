# -*- coding: utf-8 -*-

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
    """MySQL row-based binlog events publisher.

    The additional kwargs will be passed to `BinLogStreamReader`.

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
