# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import itertools
import logging

from .._compat import urlparse
from ..signals import signal


def statsd_sub(statsd_dsn, tables, prefix="meepo.stats", rate=1):
    """statsd sub for simple stats aggregation on table events.

    This sub will send stats info to statsd daemon for counter statistics.

    To use this sub, you have to install statsd python lib with::

        $ pip install statsd

    For more info about statsd, refer to:

        https://github.com/etsy/statsd/

    :param statsd_dsn: statsd server dsn
    :param tables: table stats to be collected.
    :param prefix: statsd key prefix
    :param rate: statsd sample rate, default to 100%.
    """
    logger = logging.getLogger("meepo.sub.statsd_sub")

    import statsd

    parsed = urlparse(statsd_dsn)
    c = statsd.StatsClient(parsed.hostname, parsed.port, prefix=prefix)

    _key = lambda tb, ac: '.'.join([prefix, tb, ac])

    events = ("%s_%s" % (tb, action) for tb, action in
              itertools.product(*[tables, ["write", "update", "delete"]]))
    for event in events:
        signal(event).connect(
            lambda _, e=event: c.incr(_key(*e.rsplit('_', 1)),  rate=rate),
            weak=False
        )

    logger.debug("Statsd sub hook enabled.")

    return c
