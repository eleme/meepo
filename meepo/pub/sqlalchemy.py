# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging
logger = logging.getLogger("meepo.pub.sqlalchemy_pub")

import collections
import uuid

from blinker import signal
from sqlalchemy import event


def _pk(obj):
    """Get pk values from object

    :param obj: sqlalchemy object
    """
    pk_values = tuple(getattr(obj, c.name)
                      for c in obj.__mapper__.primary_key)
    if len(pk_values) == 1:
        return pk_values[0]
    return pk_values


def _pub(obj, action):
    """Publish object pk values with action.

    The _pub will trigger 2 signals:
    * normal ``table_action`` signal, sends primary key
    * raw ``table_action_raw`` signal, sends sqlalchemy object

    :param obj: sqlalchemy object
    :param action: action on object
    """
    sg_name = "{}_{}".format(obj.__table__, action)
    sg = signal(sg_name)
    sg_raw = signal("{}_raw".format(sg_name))

    pk = _pk(obj)
    if pk:
        sg.send(pk)
        sg_raw.send(obj)
        logger.debug("{} -> {}".format(sg_name, pk))


def _session_init(session, *args, **kwargs):
    if hasattr(session, "meepo_unique_id"):
        logger.debug("skipped - session_init")
        return

    for action in ("write", "update", "delete"):
        attr = "pending_{}".format(action)
        if not hasattr(session, attr):
            setattr(session, attr, set())
    session.meepo_unique_id = uuid.uuid4().hex
    logger.debug("%s - session_init" % session.meepo_unique_id)


def _session_del(session):
    del session.meepo_unique_id
    del session.pending_write
    del session.pending_update
    del session.pending_delete


def _session_update(session, flush_ctx, nonsense):
    """Record session changes on flush
    """
    _session_init(session)
    logger.debug("%s - before_flush" % session.meepo_unique_id)
    session.pending_write |= set(session.new)
    session.pending_update |= set(session.dirty)
    session.pending_delete |= set(session.deleted)


def _session_pub(session):
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


def sqlalchemy_pub(session, tables=None):
    """SQLAlchemy Pub.

    This publisher don't publish events itself, it will hook on sqlalchemy's
    event system, and publish the changes automatically.

    :param session: sqlalchemy db session.
    :param tables: which tables to enable sqlalchemy_pub.
    """
    # enable session_update hook
    event.listen(session, "before_flush", _session_update)

    # enable simple session_commit hook
    def _session_commit(session):
        # this may happen when there's nothing to commit
        if not hasattr(session, 'meepo_unique_id'):
            logger.debug("skipped - after_commit")
            return

        _session_pub(session)
        _session_del(session)
    event.listen(session, "after_commit", _session_commit)


def sqlalchemy_es_pub(session, tables=None):
    """SQLAlchemy EventSourcing Pub.

    Add eventsourcing to sqlalchemy_pub, three more signals added for tables:

        session_prepare
        session_commit / session_rollback

    The hook will use prepare-commit pattern to ensure 100% reliability on
    event sourcing.

    :param session: sqlalchemy session.
    :param tables: which tables to enable sqlalchemy_es_pub.
    """
    # enable session_update hook
    event.listen(session, "before_flush", _session_update)

    # enable es session_prepare hook
    def _session_prepare(session, flush_ctx):
        """Record session prepare state in before_commit
        """
        if not hasattr(session, 'meepo_unique_id'):
            _session_init(session)

        logger.debug("%s - after_flush" % session.meepo_unique_id)

        evt = collections.defaultdict(set)
        for action in ("write", "update", "delete"):
            for obj in [o for o in getattr(session, "pending_%s" % action)
                        if o.__table__.fullname in tables]:
                evt_name = "%s_%s" % (obj.__table__.fullname, action)
                evt[evt_name].add(_pk(obj))
        logger.debug("%s - session_prepare %s -> %s".format(
            session.meepo_unique_id, evt_name, evt))
        signal("session_prepare").send(session, event=evt)
    event.listen(session, "after_flush", _session_prepare)

    # enable es session_commit hook
    def _session_commit(session):
        """Commit session in after_commit
        """
        # this may happen when there's nothing to commit
        if not hasattr(session, 'meepo_unique_id'):
            logger.debug("skipped - after_commit")
            return

        # normal session pub
        logger.debug("%s - after_commit" % session.meepo_unique_id)
        _session_pub(session)
        signal("session_commit").send(session)
        _session_del(session)
    event.listen(session, "after_commit", _session_commit)

    # enable es session_rollback hook
    def session_rollback(session):
        """Clean session in after_rollback.
        """
        # this may happen when there's nothing to rollback
        if not hasattr(session, 'meepo_unique_id'):
            logger.debug("skipped - after_rollback")
            return

        # del session meepo id after rollback
        logger.debug("%s - after_rollback" % session.meepo_unique_id)
        signal("session_rollback").send(session)
        _session_del(session)
    event.listen(session, "after_rollback", session_rollback)
