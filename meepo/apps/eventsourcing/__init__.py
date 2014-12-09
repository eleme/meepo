# -*- coding: utf-8 -*-

"""
For basic concept about eventsourcing, refer to
http://martinfowler.com/eaaDev/EventSourcing.html

**Simple Eventsourcing**

The eventsourcing implemented in meepo is a simplified version of es, it only
records what has changed since a timestamp, but not the diffs.

So you only get a list of primary keys when query with a timestamp::

    order_update 102 27 59 43

Because event sourcing is hard in distributed system, you can't
give a accurate answer of events happening order. So we only keep a record
of what happened since some time, then you know the data has gone stale,
and you have to retrieve latest data from source and do the tasks upon it.

**Why Eventsourcing**

Why is eventsourcing needed? Let's check the sqlalchemy_pub events flow:

a. before flush -> record instances states
b. commit transaction in database
c. after commit -> pub signal

So it's possible that the process(or thread or greenlet) somehow being killed
right between b and c, then the signal lost.

With prepare commit in event sourcing, the session will be recorded so it's
possible to recover from this corrupt state.

But you should note this is a very rare, so in most cases, you don't need
this 100% grantee on events, then just use the simple :func:`sqlalchemy_pub`
is enough.
"""

from __future__ import absolute_import

__all__ = ["sqlalchemy_es_pub", "redis_es_sub"]

from .pub import sqlalchemy_es_pub
from .sub import redis_es_sub
