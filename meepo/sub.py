# -*- coding: utf-8 -*-

from __future__ import absolute_import

import datetime
import functools
import logging
import pickle

import sqlalchemy as sa
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import SQLAlchemyError

from blinker import signal

import redis
import zmq


def print_sub(tables):
    """Events print subscriber.
    """
    logger = logging.getLogger("meepo.sub.print_sub")
    logger.info("print_sub tables: %s" % ", ".join(tables))

    for table in set(tables):
        signal("{}_write".format(table)).connect(
            lambda pk, t=table: logger.info("{}_write -> {}".format(t, pk)),
            weak=False)
        signal("{}_update".format(table)).connect(
            lambda pk, t=table: logger.info("{}_update -> {}".format(t, pk)),
            weak=False)
        signal("{}_delete".format(table)).connect(
            lambda pk, t=table: logger.info("{}_delete -> {}".format(t, pk)),
            weak=False)


def replicate_sub(master_dsn, slave_dsn, tables=None):
    """Database replication subscriber.

    This meepo event sourcing system is based upon database primary key, so
    table should have a pk here.

    The function will subscribe to the event sourcing pk stream, retrive rows
    from master based pk and then update the slave.
    """
    logger = logging.getLogger("meepo.sub.replicate_sub")

    # sqlalchemy reflection
    logger.info("reflecting master database: {}".format(master_dsn))
    master_engine = sa.create_engine(master_dsn)
    master_base = automap_base()
    master_base.prepare(engine=master_engine, reflect=True)
    MasterSession = scoped_session(sessionmaker(bind=master_engine))

    logger.info("reflecting slave database: {}".format(slave_dsn))
    slave_engine = sa.create_engine(slave_dsn)
    slave_base = automap_base()
    slave_base.prepare(engine=slave_engine, reflect=True)
    SlaveSession = scoped_session(sessionmaker(bind=slave_engine))

    def _write_by_pk(name, pk):
        """Copy row from master to slave based on pk
        """
        MasterModel = master_base.classes[name]
        obj = MasterSession.query(MasterModel).get(pk)
        if not obj:
            logger.error("pk for {} not found in master: {}".format(name, pk))
            return

        SlaveModel = slave_base.classes[name]
        columns = [c.name for c in SlaveModel.__table__.columns]
        s_obj = SlaveModel(**{k: v
                              for k, v in obj.__dict__.items()
                              if k in columns})
        SlaveSession.add(s_obj)

        try:
            SlaveSession.commit()
        except SQLAlchemyError as e:
            SlaveSession.rollback()
            logger.exception(e)

        # cleanup
        MasterSession.close()
        SlaveSession.close()

    def _update_by_pk(name, pk):
        """Update row from master to slave based on pk
        """
        MasterModel = master_base.classes[name]
        obj = MasterSession.query(MasterModel).get(pk)

        SlaveModel = slave_base.classes[name]
        s_obj = SlaveSession.query(SlaveModel).get(pk)
        if not s_obj:
            return _write_by_pk(name, pk)

        columns = [c.name for c in SlaveModel.__table__.columns]
        for col in columns:
            try:
                val = getattr(obj, col)
            except AttributeError as e:
                continue
            setattr(s_obj, col, val)

        try:
            SlaveSession.commit()
        except SQLAlchemyError as e:
            SlaveSession.rollback()
            logger.exception(e)

        # cleanup
        MasterSession.close()
        SlaveSession.close()

    def _delete_by_pk(name, pk):
        """Copy row from slave based on pk
        """
        Model = slave_base.classes[name]
        obj = SlaveSession.query(Model).get(pk)
        if obj:
            SlaveSession.delete(obj)
        SlaveSession.commit()

        # cleanup
        SlaveSession.close()

    def _sub(table):

        def _sub_write(pk):
            logger.info("dbreplica_sub {}_write: {}".format(table, pk))
            _write_by_pk(table, pk)
        signal("%s_write" % table).connect(_sub_write, weak=False)

        def _sub_update(pk):
            logger.info("dbreplica_sub {}_update: {}".format(table, pk))
            _update_by_pk(table, pk)
        signal("%s_update" % table).connect(_sub_update, weak=False)

        def _sub_delete(pk):
            logger.info("dbreplica_sub {}_delete: {}".format(table, pk))
            _delete_by_pk(table, pk)
        signal("%s_delete" % table).connect(_sub_delete, weak=False)

    if tables:
        tables = (t for t in tables if t in slave_base.classes.keys())

    for table in tables:
        _sub(table)


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


def zmq_sub(bind, tables, forwarder=False):
    """0mq fanout subscriber.

    This subscriber will use zeromq to publish the event to outside.
    """
    logger = logging.getLogger("meepo.sub.nano_sub")

    ctx = zmq.Context()
    pub_socket = ctx.socket(zmq.PUB)

    if forwarder:
        pub_socket.connect(bind)
    else:
        pub_socket.bind(bind)

    def _sub(table):
        for action in ("write", "update", "delete"):
            def _sub(pk, action=action):
                msg = "%s_%s %s" % (table, action, pk)
                pub_socket.send_string(msg)
                logger.debug("pub msg: %s" % msg)
            signal("%s_%s" % (table, action)).connect(_sub, weak=False)

    for table in set(tables):
        _sub(table)


def nano_sub(bind, tables):
    """Nanomsg fanout subscriber. (Experimental)

    This subscriber will use nanomsg to publish the event to outside.
    """
    logger = logging.getLogger("meepo.sub.nano_sub")

    from nanomsg import Socket, PUB

    pub_socket = Socket(PUB)
    pub_socket.bind(bind)

    def _sub(table):
        for action in ("write", "update", "delete"):
            def _sub(pk, action=action):
                msg = bytes("%s_%s %s" % (table, action, pk), 'utf-8')
                logger.debug("pub msg %s" % msg)
                pub_socket.send(msg)

            signal("%s_%s" % (table, action)).connect(_sub, weak=False)

    for table in set(tables):
        _sub(table)
