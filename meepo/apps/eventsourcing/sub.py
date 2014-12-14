# -*- coding: utf-8 -*-

from __future__ import absolute_import

import datetime
import functools
import itertools
import logging

from ...signals import signal

from .event_store import RedisEventStore
from .prepare_commit import RedisPrepareCommit


def redis_es_sub(session, tables, redis_dsn, strict=False,
                 namespace=None, ttl=3600*24*3, socket_timeout=1):
    """Redis EventSourcing sub.

    This sub should be used together with sqlalchemy_es_pub, it will
    use RedisEventStore as events storage layer and use the prepare-commit
    pattern in :func:`sqlalchemy_es_pub` to ensure 100% security on
    events recording.

    :param session: the sqlalchemy to bind the signal
    :param tables: tables to be event sourced.
    :param redis_dsn: the redis server to store event sourcing events.
    :param strict: arg to be passed to RedisPrepareCommit. If set to True,
     the exception will not be silent and may cause the failure of sqlalchemy
     transaction, user should handle the exception in the app side in this
     case.
    :param namespace: namespace string or func. If func passed, it should
     accept timestamp as arg and return a string namespace.
    :param ttl: expiration time for events stored, default to 3 days.
    :param socket_timeout: redis socket timeout.
    """
    logger = logging.getLogger("meepo.apps.eventsourcing.redis_es_sub")

    if not isinstance(tables, (list, set)):
        raise ValueError("tables should be list or set")

    # install event store hook for tables
    event_store = RedisEventStore(
        redis_dsn, namespace=namespace, ttl=ttl, socket_timeout=socket_timeout)

    def _es_event_sub(pk, event):
        if event_store.add(event, str(pk)):
            logger.info("%s: %s -> %s" % (
                event, pk, datetime.datetime.now()))
        else:
            logger.error("event sourcing failed: %s" % pk)

    events = ("%s_%s" % (tb, action) for tb, action in
              itertools.product(*[tables, ["write", "update",  "delete"]]))
    for event in events:
        sub_func = functools.partial(_es_event_sub, event=event)
        signal(event).connect(sub_func, weak=False)

    # install prepare-commit hook
    prepare_commit = RedisPrepareCommit(
        redis_dsn, strict=strict, namespace=namespace,
        socket_timeout=socket_timeout)

    signal("session_prepare").connect(
        prepare_commit.prepare, sender=session, weak=False)
    signal("session_commit").connect(
        prepare_commit.commit, sender=session, weak=False)
    signal("session_rollback").connect(
        prepare_commit.rollback, sender=session, weak=False)

    return event_store, prepare_commit
