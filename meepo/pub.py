# -*- coding: utf-8 -*-

import logging

from urllib.parse import urlparse

from blinker import signal


def mysql_pub(mysql_dsn, tables=None, **kwargs):
    """MySQL row-based binlog events publisher.

    The additional kwargs will be passed to `BinLogStreamReader`.
    """
    # only import when used to avoid dependency requirements
    import pymysqlreplication
    from pymysqlreplication.row_event import (
        DeleteRowsEvent,
        UpdateRowsEvent,
        WriteRowsEvent,
    )

    logger = logging.getLogger("meepo.pub.mysql_pub")

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
        connection_settings=mysql_settings,
        resume_stream=True,
        blocking=True,
        only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent],
        **kwargs
    )

    def _gen_stream():
        """Use gen_stream to try-except wrap a stream generator to make sure
        it always keeps on running.
        """
        while True:
            try:
                yield from stream
            except KeyError as e:
                logger.info(str(e))

    def _pk(values):
        if isinstance(event.primary_key, str):
            return values[event.primary_key]
        return tuple(values[k] for k in event.primary_key)

    for event in _gen_stream():
        try:
            rows = event.rows
        except (UnicodeDecodeError, ValueError) as e:
            logger.error(e)
            continue

        if tables and event.table not in tables:
            continue

        if isinstance(event, WriteRowsEvent):
            sg_name = "{}_write".format(event.table)
            sg = signal(sg_name)

            for row in rows:
                pk = _pk(row["values"])
                sg.send(pk)

                logger.debug("{} -> {}".format(sg_name, pk))

        elif isinstance(event, UpdateRowsEvent):
            sg_name = "{}_update".format(event.table)
            sg = signal(sg_name)

            for row in rows:
                pk = _pk(row["after_values"])
                sg.send(pk)

                logger.debug("{} -> {}".format(sg_name, pk))

        elif isinstance(event, DeleteRowsEvent):
            sg_name = "{}_delete".format(event.table)
            sg = signal(sg_name)

            for row in rows:
                pk = _pk(row["values"])
                sg.send(pk)

                logger.debug("{} -> {}".format(sg_name, pk))


def sqlalchemy_pub(dbsession, strict=False):
    """SQLAlchemy events publisher.

    This publisher don't publish events itself, it will hook on sqlalchemy's
    event system, and publish the changes automatically.

    When `strict` set to True, it will use a prepare-commit pattern to help
    ensure 100% reliablity on event sourcing (and broadcasting), not this will
    need another monitor daemon to monitor the states. For more info about
    this patter, refer to documentation.
    """
    from sqlalchemy import event

    logger = logging.getLogger("meepo.pub.sqlalchemy_pub")

    def _pk(obj):
        """Get pk values from object
        """
        pk_values = tuple(getattr(obj, c.name)
                          for c in obj.__mapper__.primary_key)
        if len(pk_values) == 1:
            return pk_values[0]
        return pk_values

    def _pub(obj, action):
        """Publish object pk values with action.
        """
        sg_name = "{}_{}".format(obj.__table__, action)
        sg = signal(sg_name)

        pk = _pk(obj)
        if pk:
            sg.send(pk)
            logger.debug("{} -> {}".format(sg_name, pk))

    def after_begin_hook(session, transaction, connection):
        """Init pending sets
        """
        session.pending_write = set()
        session.pending_update = set()
        session.pending_delete = set()
    event.listen(dbsession, "after_begin", after_begin_hook)

    def after_flush_hook(session, flush_ctx):
        """Record session changes on flush
        """
        session.pending_write |= set(session.new)
        session.pending_update |= set(session.dirty)
        session.pending_delete |= set(session.deleted)
    event.listen(dbsession, "after_flush", after_flush_hook)

    def after_commit_hook(session):
        """Publish signals
        """
        for obj in session.pending_write:
            _pub(obj, action="write")
        for obj in session.pending_update:
            _pub(obj, action="update")
        for obj in session.pending_delete:
            _pub(obj, action="delete")

        del session.pending_write
        del session.pending_update
        del session.pending_delete
    event.listen(dbsession, "after_commit", after_commit_hook)
