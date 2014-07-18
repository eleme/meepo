# -*- coding: utf-8 -*-

import functools
import logging
import time

import sqlalchemy as sa
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.automap import automap_base

from blinker import signal

import redis


def print_sub(tables):
    """Events print subscriber.
    """
    logger = logging.getLogger("meepo.sub.print_sub")
    logger.info("print_sub tables: %s" % ", ".join(tables))

    for table in set(tables):
        _print = lambda pk, t=table: logger.info("{} -> {}".format(t, pk))
        signal("{}_write".format(table)).connect(_print, weak=False)
        signal("{}_update".format(table)).connect(_print, weak=False)
        signal("{}_delete".format(table)).connect(_print, weak=False)


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
        SlaveSession.commit()

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
            setattr(s_obj, col, getattr(obj, col))
        SlaveSession.commit()

        # cleanup
        MasterSession.close()
        SlaveSession.close()

    def _delete_by_pk(name, pk):
        """Copy row from slave based on pk
        """
        Model = slave_base.classes[name]
        obj = SlaveSession.query(Model).get(pk)
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


def es_sub(redis_dsn, tables, namespace=None):
    """EventSourcing subscriber.

    This subscriber will use redis as event sourcing storage layer.

    Note here we only needs a 'weak' event sourcing, we only record primary
    keys, which means we only care about what event happend after some time,
    and ignore how many times it happens.
    """
    logger = logging.getLogger("meepo.sub.es_sub")
    namespace = namespace or "meepo:es_sub"

    r = redis.StrictRedis.from_url(redis_dsn)

    for table in set(tables):
        def _sub(action, pk, table=table):
            logger.info("es_sub %s_%s: %s" % (table, action, pk))
            key = "%s:%s_%s" % (namespace, table, action)
            r.zadd(key, time.time(), str(pk))

        signal("%s_write" % table).connect(
            functools.partial(_sub, "write"), weak=False)
        signal("%s_update" % table).connect(
            functools.partial(_sub, "update"), weak=False)
        signal("%s_delete" % table).connect(
            functools.partial(_sub, "delete"), weak=False)
