# -*- coding: utf-8 -*-

from __future__ import absolute_import

import logging

import collections

from sqlalchemy import event

from ...pub.sqlalchemy import SQLAlchemyPub
from ...signals import signal


class SQLAlchemyEventSourcingPub(SQLAlchemyPub):
    """SQLAlchemy EventSourcing Pub.

    Add eventsourcing to sqlalchemy_pub, three more signals added for tables:

    * ``session_prepare``
    * ``session_commit`` / ``session_rollback``

    The hook will use prepare-commit pattern to ensure 100% reliability on
    event sourcing.
    """
    logger = logging.getLogger("meepo.pub.sqlalchemy_es_pub")

    @classmethod
    def session_prepare(cls, session, _):
        """Send session_prepare signal in session "before_commit".

        The signal contains another event argument, which records whole info
        of what's changed in this session, so the signal receiver can receive
        and record the event.
        """
        if not hasattr(session, 'meepo_unique_id'):
            cls._session_init(session)

        tables = session.info["meepo_es_tables"]
        evt = collections.defaultdict(set)
        for action in ("write", "update", "delete"):
            objs = getattr(session, "pending_%s" % action)
            # filter tables if possible
            for obj in [o for o in objs if o.__table__.fullname in tables]:
                evt_name = "%s_%s" % (obj.__table__.fullname, action)
                evt[evt_name].add(cls._pk(obj))
                cls.logger.debug("%s - session_prepare: %s -> %s" % (
                    session.meepo_unique_id, evt_name, evt))
        signal("session_prepare").send(session, event=evt)

    @classmethod
    def session_commit(cls, session):
        """Send session_commit signal in sqlalchemy ``before_commit``.

        This marks the success of session so the session may enter commit
        state.
        """
        # this may happen when there's nothing to commit
        if not hasattr(session, 'meepo_unique_id'):
            cls.logger.debug("skipped - session_commit")
            return

        # normal session pub
        cls.logger.debug("%s - session_commit" % session.meepo_unique_id)
        cls._session_pub(session, session.info["meepo_es_tables"])
        signal("session_commit").send(session)
        cls._session_del(session)

    @classmethod
    def session_rollback(cls, session):
        """Send session_rollback signal in sqlalchemy ``after_rollback``.

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
    def install(cls, session, tables):
        """Install sqlalchemy eventsourcing hooks

        :param session: sqlalchemy session to install the hook
        :param tables: tables to install the hook, leave None to pub all.
        """
        if "meepo_es_tables" not in session.info:
            session.info["meepo_es_tables"] = set(tables)

            event.listen(session, "before_flush", cls.session_update)

            # enable session prepare-commit hook
            event.listen(session, "after_flush", cls.session_prepare)
            event.listen(session, "after_commit", cls.session_commit)
            event.listen(session, "after_rollback", cls.session_rollback)
        else:
            session.info["meepo_es_tables"] |= set(tables)


def sqlalchemy_es_pub(session, tables):
    """Install sqlalchemy eventsourcing hook by SQLAlchemyEventSourcingPub.

    :param session: sqlalchemy session.
    :param tables: which tables to enable sqlalchemy_es_pub.
    """
    SQLAlchemyEventSourcingPub().install(session, tables)
