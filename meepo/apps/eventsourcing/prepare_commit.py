# -*- coding: utf-8 -*-

"""
meepo.apps.eventsourcing.prepare_commit
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Prepare Commit feature for meepo events. (Also known as Two-Phase Commit)

For basic concept about two phase commit, refer to
http://en.wikipedia.org/wiki/Two-phase_commit_protocol

The two phase commit feature implemented in meepo is used to make sure event
100% reliably recorded in eventsourcing, and it's not a strict traditional
two-phase commit.

Only use it if you need a 100% grantee of not losing any events. The feature
should only be used in combination of sqlalchemy_es_pub, which ships with
session prepare-commit signals.
"""

from __future__ import absolute_import

import functools
import logging
import pickle
import time

import redis

from ...utils import d, s


class MPrepareCommit(object):
    """Prepare-Commit base class, defines the essential APIs.
    """
    def __init__(self):
        pass

    def prepare(self):
        raise NotImplementedError

    def commit(self):
        raise NotImplementedError

    def rollback(self):
        pass


def _redis_strict_pc(func):
    """Strict deco for MRedisPrepareCommit

    The deco will choose whether to silent exception or not based on the
    strict attr in MRedisPrepareCommit object.
    """
    phase = "session_%s" % func.__name__

    @functools.wraps(func)
    def wrapper(self, session, *args, **kwargs):
        try:
            func(self, session, *args, **kwargs)
            self.logger.debug("%s -> %s" % (phase, session.meepo_unique_id))
            return True
        except Exception as e:
            if self.strict:
                raise
            if isinstance(e, redis.ConnectionError):
                self.logger.warn("redis connection error in %s: %s" % (
                    phase, session.meepo_unique_id))
            else:
                self.logger.exception(e)
            return False
    return wrapper


class MRedisPrepareCommit(MPrepareCommit):
    """Prepare Commit based on redis.

    This prepare commit records sqlalchemy session, and should be used with
    meepo.pub.sqlalchemy_es_pub.
    """
    def __init__(self, redis_dsn, strict=False, namespace=None, ttl=3600*24,
                 socket_timeout=1, **kwargs):
        """Init MRedisPrepareCommit

        :param redis_dsn: the redis instance uri
        :param strict: by default the exceptions happened in middle of
         prepare-commit will only be caught and logged as error, but the
         process continue to execute. If strict set to True, the exception
         will be  raised to outside.
        :param namespace: namespace string or namespace func. if func passed,
         it should accepts timestamp as arg and return string namespace.
        :param ttl: expiration time for events stored, default to 1 day.
        :param socket_timeout: redis socket timeout
        :param kwargs: kwargs to be passed to redis instance init func.
        """
        super(MRedisPrepareCommit, self).__init__()

        self.r = redis.StrictRedis.from_url(
            redis_dsn, socket_timeout=socket_timeout, **kwargs)
        self.strict = strict
        self.ttl = ttl
        self.logger = logging.getLogger("meepo.redis_pc")

        if namespace is None:
            self.namespace = lambda ts: "meepo:redis_pc:%s" % d(ts, "%Y%m%d")
        elif isinstance(namespace, str):
            self.namespace = lambda ts: namespace
        elif callable(namespace):
            self.namespace = namespace

    def _keygen(self, session):
        if not hasattr(session, "meepo_prepare_ts"):
            session.meepo_prepare_ts = int(time.time())
        prefix = self.namespace(session.meepo_prepare_ts)
        sp_key = "%s:session_prepare" % prefix
        sp_hkey = "%s:%s" % (sp_key, session.meepo_unique_id)
        return sp_key, sp_hkey

    def phase(self, session):
        """Determine the session phase in prepare commit.

        :param session: sqlalchemy session
        :return: phase "prepare" or "commit"
        """
        sp_key, _ = self._keygen(session)
        if self.r.sismember(sp_key, session.meepo_unique_id):
            return "prepare"
        else:
            return "commit"

    @_redis_strict_pc
    def prepare(self, session, event):
        """Prepare phase for session.

        :param session: sqlalchemy session
        """
        sp_key, sp_hkey = self._keygen(session)
        pickled_event = {k: pickle.dumps(v) for k, v in event.items()}
        with self.r.pipeline(transaction=False) as p:
            p.sadd(sp_key, session.meepo_unique_id)
            p.hmset(sp_hkey, pickled_event)
            p.execute()

    @_redis_strict_pc
    def commit(self, session):
        """Commit phase for session.

        :param session: sqlalchemy session
        """
        sp_key, sp_hkey = self._keygen(session)
        with self.r.pipeline(transaction=False) as p:
            p.srem(sp_key, session.meepo_unique_id)
            p.expire(sp_hkey, 60 * 60)
            p.execute()
    # we don't need to specially deal with rollback in this phase
    rollback = commit

    def get_session_info(self, session):
        """Return all session unique ids recorded in prepare phase.

        :param ts: timestamp, default to current timestamp
        :return: set of session unique ids
        """
        _, sp_hkey = self._keygen(session)
        picked_event = self.r.hgetall(sp_hkey)
        event = {s(k): pickle.loads(v) for k, v in picked_event.items()}
        return event

    def get_prepare_info(self, ts=None):
        """Return all session unique ids recorded in prepare phase.

        :param ts: timestamp, default to current timestamp
        :return: set of session unique ids
        """
        sp_key = "%s:session_prepare" % self.namespace(ts or int(time.time()))
        return set(s(m) for m in self.r.smembers(sp_key))
