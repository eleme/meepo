# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
logger = logging.getLogger("meepo.pub.sqlalchemy_pub")

import uuid

from blinker import signal
from sqlalchemy import event


def sa_pk(obj):
    """Get pk values from object

    :param obj: sqlalchemy object
    """
    pk_values = tuple(getattr(obj, c.name)
                      for c in obj.__mapper__.primary_key)
    if len(pk_values) == 1:
        return pk_values[0]
    return pk_values


def session_init(session, *args, **kwargs):
    if hasattr(session, "meepo_unique_id"):
        logger.debug("skipped - session_init")
        return

    for action in ("write", "update", "delete"):
        attr = "pending_{}".format(action)
        if not hasattr(session, attr):
            setattr(session, attr, set())
    session.meepo_unique_id = uuid.uuid4().hex
    logger.debug("%s - session_init" % session.meepo_unique_id)


def session_del(session):
    del session.meepo_unique_id
    del session.pending_write
    del session.pending_update
    del session.pending_delete


def session_update(session, flush_ctx, nonsense):
    """Record session changes on flush
    """
    session_init(session)
    logger.debug("%s - before_flush" % session.meepo_unique_id)
    session.pending_write |= set(session.new)
    session.pending_update |= set(session.dirty)
    session.pending_delete |= set(session.deleted)


def session_pub(session):
    def _pub(obj, action, tables=None):
        """Publish object pk values with action.

        The _pub will trigger 2 signals:
        * normal ``table_action`` signal, sends primary key
        * raw ``table_action_raw`` signal, sends sqlalchemy object

        :param obj: sqlalchemy object
        :param action: action on object
        """
        if tables and obj.__tablename__ not in tables:
            return

        sg_name = "{}_{}".format(obj.__table__, action)
        sg = signal(sg_name)
        sg_raw = signal("{}_raw".format(sg_name))

        pk = sa_pk(obj)
        if pk:
            sg.send(pk)
            sg_raw.send(obj)
            logger.debug("{} -> {}".format(sg_name, pk))

    logger.debug("pub_session")
    for obj in session.pending_write:
        _pub(obj, action="write")
    for obj in session.pending_update:
        _pub(obj, action="update")
    for obj in session.pending_delete:
        _pub(obj, action="delete")

    session.pending_write.clear()
    session.pending_update.clear()
    session.pending_delete.clear()


def session_commit(session):
    # this may happen when there's nothing to commit
    if not hasattr(session, 'meepo_unique_id'):
        logger.debug("skipped - after_commit")
        return

    session_pub(session)
    session_del(session)


def sqlalchemy_pub(session, tables=None):
    """SQLAlchemy Pub.

    This publisher don't publish events itself, it will hook on sqlalchemy's
    event system, and publish the changes automatically.

    :param session: sqlalchemy db session.
    :param tables: which tables to enable sqlalchemy_pub.
    """
    # enable session_update & session_commit hook
    event.listen(session, "before_flush", session_update)
    event.listen(session, "after_commit", session_commit)
