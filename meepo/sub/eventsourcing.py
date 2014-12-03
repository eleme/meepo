# -*- coding: utf-8 -*-

import datetime
import functools
import logging

from blinker import signal
import redis

from .._compat import pickle


def es_sub(redis_dsn, tables, namespace=None, ttl=3600*24*3):
    """EventSourcing subscriber.

    This subscriber will use redis as event sourcing storage layer.

    Note here we only needs a 'weak' event sourcing, we only record primary
    keys with lastest change timestamp, which means we only care about
    what event happend after some time, and ignore how many times it happens.
    """
    logger = logging.getLogger("meepo.sub.es_sub")

    # we may accept function as namespace so we could dynamically generate it.
    # if namespace provided as string, the function return the string.
    # elif namespace not provided, generate namespace dynamically by today.
    if not callable(namespace):
        namespace = lambda: namespace if namespace else \
            "meepo:es:{}".format(datetime.date.today())

    r = redis.StrictRedis.from_url(
        redis_dsn, socket_timeout=1, socket_connect_timeout=0.1)

    LUA_ZADD = ' '.join("""
    local score = redis.call('ZSCORE', KEYS[1], ARGV[2])
    if score and tonumber(ARGV[1]) <= tonumber(score) then
        return 0
    else
        redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
        return 1
    end
    """.split())
    r_time = lambda: r.eval("return tonumber(redis.call('TIME')[1])", 1, 1)
    r_zadd = lambda k, pk: r.eval(LUA_ZADD, 1, k, r_time(), pk)

    for table in set(tables):
        def _sub(action, pk, table=table):
            key = "%s:%s_%s" % (namespace(), table, action)
            try:
                if r_zadd(key, str(pk)):
                    logger.info("%s_%s: %s -> %s" % (
                        table, action, pk,
                        datetime.datetime.now()))
                else:
                    logger.info("%s_%s: %s -> skip" % (table, action, pk))
            except redis.ConnectionError:
                logger.error("event sourcing failed: %s" % pk)
            except Exception as e:
                logger.exception(e)

        signal("%s_write" % table).connect(
            functools.partial(_sub, "write"), weak=False)
        signal("%s_update" % table).connect(
            functools.partial(_sub, "update"), weak=False)
        signal("%s_delete" % table).connect(
            functools.partial(_sub, "delete"), weak=False)

    def _clean_sid(sid):
        sp_all = "%s:session_prepare" % namespace()
        sp_key = "%s:session_prepare:%s" % (namespace(), sid)
        try:
            with r.pipeline(transaction=False) as p:
                p.srem(sp_all, sid)
                p.expire(sp_key, 60 * 60)
                p.execute()
            return True
        except redis.ConnectionError:
            logger.warn(
                "redis connection error in session commit/rollback: %s" %
                sid)
            return False
        except Exception as e:
            logger.exception(e)
            return False

    # session hooks for strict prepare-commit pattern
    def session_prepare_hook(event, sid, action):
        """Record session prepare state.
        """
        sp_all = "%s:session_prepare" % namespace()
        sp_key = "%s:session_prepare:%s" % (namespace(), sid)

        try:
            with r.pipeline(transaction=False) as p:
                p.sadd(sp_all, sid)
                p.hset(sp_key, action, pickle.dumps(event))
                p.execute()
            logger.info("session_prepare %s -> %s" % (action, sid))
        except redis.ConnectionError:
            logger.warn("redis connection error in session prepare: %s" % sid)
        except Exception as e:
            logger.exception(e)
    signal("session_prepare").connect(session_prepare_hook, weak=False)

    def session_commit_hook(sid):
        if _clean_sid(sid):
            logger.info("session_commit -> %s" % sid)
        else:
            logger.warn("session_commit failed -> %s" % sid)
    signal("session_commit").connect(session_commit_hook, weak=False)

    def session_rollback_hook(sid):
        if _clean_sid(sid):
            logger.info("session_rollback -> %s" % sid)
        else:
            logger.warn("session_rollback failed -> %s" % sid)
    signal("session_rollback").connect(session_rollback_hook, weak=False)
