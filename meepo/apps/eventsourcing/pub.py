# -*- coding: utf-8 -*-

"""
EventSourcing - Pub
-------------------

Pubs for meepo eventsourcing app.
"""

from __future__ import absolute_import

import logging

import collections

from blinker import signal
from sqlalchemy import event

from ...pub.sqlalchemy import MSQLAlchemyPub


class MSQLAlchemyEventSourcingPub(MSQLAlchemyPub):
    """SQLAlchemy EventSourcing Pub.

    Add eventsourcing to sqlalchemy_pub, three more signals added for tables:

    * session_prepare
    * session_commit / session_rollback

    The hook will use prepare-commit pattern to ensure 100% reliability on
    event sourcing.
    """
    logger = logging.getLogger("meepo.apps.eventsourcing.sqlalchemy_pub")

    @classmethod
    def session_prepare(cls, session, _):
        """Send session_prepare signal in session "before_commit".

        The signal contains another event argument, which records whole info
        of what's changed in this session, so the signal receiver can receive
        and record the event.
        """
        if not hasattr(session, 'meepo_unique_id'):
            cls._session_init(session)

        tables = getattr(session, "_meepo_sqlalchemy_es_pub_tables", [])
        evt = collections.defaultdict(set)
        for action in ("write", "update", "delete"):
            objs = getattr(session, "pending_%s" % action)
            # filter tables if possible
            if tables:
                objs = [o for o in objs if o.__table__.fullname in tables]
            for obj in objs:
                evt_name = "%s_%s" % (obj.__table__.fullname, action)
                evt[evt_name].add(cls._pk(obj))
                cls.logger.debug("%s - session_prepare: %s -> %s".format(
                    session.meepo_unique_id, evt_name, evt))
        signal("session_prepare").send(session, event=evt)

    @classmethod
    def session_commit(cls, session):
        """Send session_commit signal in sqlalchemy "before_commit".

        This marks the success of session so the session may enter commit
        state.
        """
        # this may happen when there's nothing to commit
        if not hasattr(session, 'meepo_unique_id'):
            cls.logger.debug("skipped - session_commit")
            return

        # normal session pub
        cls.logger.debug("%s - session_commit" % session.meepo_unique_id)
        cls._session_pub(session)
        signal("session_commit").send(session)
        cls._session_del(session)

    @classmethod
    def session_rollback(cls, session):
        """Send session_rollback signal in sqlalchemy "after_rollback".

        This marks the failure of session so the session may enter commit
        phase.
        """
        # this may happen when there's nothing to rollback
        if not hasattr(session, 'meepo_unique_id'):
            cls.logger.debug("skipped - session_rollback")
            return

        # del session meepo id after rollback
        cls.logger.debug("%s - after_rollback" % session.meepo_unique_id)
        signal("session_rollback").send(session)
        cls._session_del(session)

    @classmethod
    def install(cls, session, tables=None):
        """Install sqlalchemy eventsourcing hooks.

        :param session: sqlalchemy session to install the hook
        :param tables: tables to install the hook, leave None to pub all.
        """
        cls.logger.debug("session_install - eventsourcing")

        if not hasattr(session, "_meepo_sqlalchemy_es_pub_tables"):
            # init es listen tables
            session._meepo_sqlalchemy_es_pub_tables = set()

            # enable session_update hook
            event.listen(session, "before_flush", cls.session_update)

            # enable session prepare-commit hook
            event.listen(session, "after_flush", cls.session_prepare)
            event.listen(session, "after_commit", cls.session_commit)
            event.listen(session, "after_rollback", cls.session_rollback)

        if tables:
            session._meepo_sqlalchemy_es_pub_tables |= set(tables)


def sqlalchemy_es_pub(session, tables=None):
    """Install sqlalchemy eventsourcing hook by MSQLAlchemyEventSourcingPub.

    :param session: sqlalchemy session.
    :param tables: which tables to enable sqlalchemy_es_pub.
    """
    MSQLAlchemyEventSourcingPub().install(session, tables)
