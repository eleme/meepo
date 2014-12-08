# -*- coding: utf-8 -*-

"""
EventSourcing - EventStore
--------------------------

For basic concept about eventsourcing, refer to
http://martinfowler.com/eaaDev/EventSourcing.html

The eventsourcing implemented in meepo is a simplified version of es, it only
records what has changed since a timestamp, but not the diffs.

So you only get a list of primary keys when query with a timestamp::

    order_update 102 27 59 43

Why is it? Because event sourcing is hard in distributed system, you can't
give a accurate answer of events happening order. So we only keep a record
that it happened since some timestamp, then you know the data has gone stale,
and you have to retrieve latest data from database and do the following
tasks next.
"""

from __future__ import absolute_import

import logging
import time

import redis

from ...utils import s, d


class MEventStore(object):
    def __init__(self):
        pass

    def add(self, event, pk, ts=None):
        pass

    def query(self, event, ts=None):
        pass


class MRedisEventStore(MEventStore):
    """EventStore based on redis.

    .. note::

        The redis event store class is compat with twemproxy.

    :param redis_dsn: the redis instance uri
    :param namespace: namespace func for event key, the func should accept
     event timestamp and return namespace of the func. namespace also
     accepts str type arg, which will always return the same namespace
     for all timestamps.
    :param ttl: expiration time for events stored, default to 3 days.
    :param socket_timeout: redis socket timeout
    :param kwargs: kwargs to be passed to redis instance init func.
    """

    LUA_TIME = "return tonumber(redis.call('TIME')[1])"
    LUA_ZADD = ' '.join("""
    local score = redis.call('ZSCORE', KEYS[1], ARGV[2])
    if score and tonumber(ARGV[1]) <= tonumber(score) then
        return 0
    else
        redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
        return 1
    end
    """.split())

    def __init__(self, redis_dsn, namespace=None, ttl=3600*24*3,
                 socket_timeout=1, **kwargs):
        super(MRedisEventStore, self).__init__()

        self.r = redis.StrictRedis.from_url(
            redis_dsn, socket_timeout=socket_timeout, **kwargs)
        self.ttl = ttl
        self.logger = logging.getLogger("meepo.redis_es")

        if namespace is None:
            self.namespace = lambda ts: "meepo:redis_es:%s" % d(ts, "%Y%m%d")
        elif isinstance(namespace, str):
            self.namespace = lambda ts: namespace
        elif callable(namespace):
            self.namespace = namespace

    def _keygen(self, event, ts=None):
        """Generate redis key for event at timestamp.

        :param event: event name
        :param ts: timestamp, default to current timestamp if left as None
        """
        return "%s:%s" % (self.namespace(ts or time.time()), event)

    def _time(self):
        """Redis lua func to get timestamp from redis server, use this func to
        prevent time inconsistent across servers.
        """
        return self.r.eval(self.LUA_TIME, 1, 1)

    def _zadd(self, key, pk, ts=None, ttl=None):
        """Redis lua func to add an event to the corresponding sorted set.

        :param key: the key to be stored in redis server
        :param pk: the primary key of event
        :param ts: timestamp of the event, default to redis_server's
         current timestamp
        :param ttl: the expiration time of event since the last update
        """
        return self.r.eval(self.LUA_ZADD, 1, key, ts or self._time(), pk)

    def add(self, event, pk, ts=None, ttl=None):
        """Add an event to event store.

        All events were stored in a sorted set in redis with timestamp as
        rank  score.

        :param event: the event to be added, format should be ``table_action``
        :param pk: the primary key of event
        :param ts: timestamp of the event, default to redis_server's
         current timestamp
        :param ttl: the expiration time of event since the last update
        :return: bool
        """
        key = self._keygen(event, ts)
        try:
            self._zadd(key, pk, ts, ttl)
            return True
        except redis.ConnectionError as e:
            # connection error typically happens when redis server can't be
            # reached or timed out, the error will be silent with an error
            # log and return None.
            self.logger.error(
                "redis event store failed with connection error %r" % e)
            return False

    def replay(self, event, ts=0, end_ts=None, with_ts=False):
        """Replay events based on timestamp.

        If you split namespace with ts, the replay will only return events
        within the same namespace.

        :param event: event name
        :param ts: replay events after ts, default from 0.
        :param end_ts: replay events to ts, default to "+inf".
        :param with_ts: return timestamp with events, default to False.
        :return: list of pks when with_ts set to False, list of (pk, ts) tuples
         when with_ts is True.
        """
        key = self._keygen(event, ts)
        end_ts = end_ts if end_ts else "+inf"
        elements = self.r.zrangebyscore(key, ts, end_ts, withscores=with_ts)

        if not with_ts:
            return [s(e) for e in elements]
        else:
            return [(s(e[0]), int(e[1])) for e in elements]

    def clear(self, event, ts=None):
        """Clear all stored record of event.

        :param event: event name to be cleared.
        :param ts: timestamp used locate the namespace
        """
        return self.r.delete(self._keygen(event, ts))
