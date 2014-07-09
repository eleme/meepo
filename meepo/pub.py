# -*- coding: utf-8 -*-

import logging

from urllib.parse import urlparse

from blinker import signal


def mysql_pub(mysql_dsn, **kwargs):
    """Use mysql row based binlog events publisher.

    The additional kwargs will be passed to `BinLogStreamReader`.
    """
    # only import when used to avoid dependency requirements
    import pymysqlreplication
    from pymysqlreplication.row_event import (
        DeleteRowsEvent,
        UpdateRowsEvent,
        WriteRowsEvent,
    )

    logger = logging.getLogger("sareplica.pub.mysql_binlog_pub")

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
        return (values[k] for k in event.primary_key)

    for event in _gen_stream():
        try:
            rows = event.rows
        except (UnicodeDecodeError, ValueError) as e:
            logger.error(e)
            continue

        if isinstance(event, WriteRowsEvent):
            sg_name = "{}_write".format(event.table)
            sg = signal(sg_name)

            for row in rows:
                pk = _pk(row["values"])
                sg.send(pk)

                logger.debug("mysql_pub {} -> {}".format(sg_name, pk))

        elif isinstance(event, UpdateRowsEvent):
            sg_name = "{}_update".format(event.table)
            sg = signal(sg_name)

            for row in rows:
                pk = _pk(row["after_values"])
                sg.send(pk)

                logger.debug("mysql_pub {} -> {}".format(sg_name, pk))

        elif isinstance(event, DeleteRowsEvent):
            sg_name = "{}_delete".format(event.table)
            sg = signal(sg_name)

            for row in rows:
                pk = _pk(row["values"])
                sg.send(pk)

                logger.debug("mysql_pub {} -> {}".format(sg_name, pk))
