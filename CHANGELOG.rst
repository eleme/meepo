Meepo Changelog
===============

Version 0.1.7
-------------

Released on October 30, 2014.

- compatiable with twemproxy


Version 0.1.6
-------------

Released on September 23, 2014.

- graceful handle KeyboardInterrupt.
- better worker retry handling.
- allow multiple pks to be sent to callback task.


Version 0.1.5
-------------

Released on September 12, 2014.

- upgrade mysql-replication version to latest.
- graceful bypass event sourcing when redis fail.
- skip mysql row event if no primary_key found.
- tests, some bugfixes and tunings.


Version 0.1.4
-------------

Released on September 2, 2014.

- now print queue size in logging message
- allow multiple workers (consisten hash on pk) for event


Version 0.1.3
-------------

Released on August 29, 2014.

- auto expire for eventsourcing sub keys.
- allow callable as namespace.


Version 0.1.2
-------------

Released on August 15, 2014.

- allow multiple topics in registered callback.


Version 0.1.1
-------------

Released on August 7, 2014.

- add meepo replicator base class.
- bug fix for sqlalchemy_pub


Version 0.1.0
-------------

Released on July 29, 2014.

First public release.
