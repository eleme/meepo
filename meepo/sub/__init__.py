# -*- coding: utf-8 -*-

"""
Meepo sub is where all the imagination comes true, all subs implemented here
are only some simple demos. Customize your own sub for the real power.

To make use of a signal, just create a function that accepts a primary key.

For example, print an event with::

    # use weak False here to force strong ref to the lambda func.
    signal("test_write").connect(
        lambda pk: logger.info("%s -> %s" % event, pk),
        weak=False
    )

For advanced use with sqlalchemy, you may also use the raw signal::

    signal("test_write_raw").connect(
        lambda obj: logger.info("%s -> %s" % event, obj.__dict__),
        weak=False)
    )
"""
