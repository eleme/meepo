# -*- coding: utf-8 -*-

"""
meepo.apps.eventsourcing.pub
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pubs for meepo eventsourcing app.
"""

from __future__ import absolute_import

import collections
import logging

from blinker import signal
from sqlalchemy import event

from ...pub.sqlalchemy import (
    session_init, session_update, session_del, session_pub, sa_pk)


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
    logger = logging.getLogger("meepo.apps.es.pub.sqlalchemy_pub")

    # enable session_update hook
    event.listen(session, "before_flush", session_update)

    # enable es session_prepare hook
    def _session_prepare(session, flush_ctx):
        """Record session prepare state in before_commit
        """
        if not hasattr(session, 'meepo_unique_id'):
            session_init(session)

        logger.debug("%s - after_flush" % session.meepo_unique_id)

        evt = collections.defaultdict(set)
        for action in ("write", "update", "delete"):
            for obj in [o for o in getattr(session, "pending_%s" % action)
                        if o.__table__.fullname in tables]:
                evt_name = "%s_%s" % (obj.__table__.fullname, action)
                evt[evt_name].add(sa_pk(obj))
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
        session_pub(session)
        signal("session_commit").send(session)
        session_del(session)
    event.listen(session, "after_commit", _session_commit)

    # enable es session_rollback hook
    def _session_rollback(session):
        """Clean session in after_rollback.
        """
        # this may happen when there's nothing to rollback
        if not hasattr(session, 'meepo_unique_id'):
            logger.debug("skipped - after_rollback")
            return

        # del session meepo id after rollback
        logger.debug("%s - after_rollback" % session.meepo_unique_id)
        signal("session_rollback").send(session)
        session_del(session)
    event.listen(session, "after_rollback", _session_rollback)
