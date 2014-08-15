#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

# console_scripts
cmds = [
    "meventsourcing = meepo.apps.meventsourcing:main",
    "mnano = meepo.apps.mnano:main",
    "mprint = meepo.apps.mprint:main",
    "mreplicate = meepo.apps.mreplicate:main",
    "mzdevice = meepo.apps.mzdevice:main",
    "mzmq = meepo.apps.mzmq:main",
]


# requirements
install_requires = [
    "SQLAlchemy>=0.9.0,<1.0.0",
    "blinker>=1.3,<2.0",
    "click>=2.4,<2.5",
    "pyzmq>=14.3.1,<14.4.0",
    "redis>=2.10.1,<2.11.0",
]

mysqlbinlog_requires = [
    "mysql-replication==0.3.0",
]

# nanomsg is still in beta
nanomsg_requires = [
    "nanomsg",
]

dev_requires = [
    "flake8>=2.2.1",
    "sphinx-rtd-theme>=0.1.6",
    "sphinx>=1.2.2",
] + mysqlbinlog_requires


setup(name="meepo",
      version="0.1.2",
      description="event sourcing for databases.",
      keywords="eventsourcing event sourcing replication cache elasticsearch",
      author="Lx Yu",
      author_email="i@lxyu.net",
      packages=find_packages(exclude=['docs', 'tests']),
      entry_points={"console_scripts": cmds},
      url="https://github.com/eleme/meepo",
      license="MIT",
      zip_safe=False,
      long_description=open("README.rst").read(),
      install_requires=install_requires,
      extras_require={
          "dev": dev_requires,
          "mysqlbinlog": mysqlbinlog_requires,
          "nano": nanomsg_requires,
      },
      classifiers=[
          "Topic :: Software Development",
          "Development Status :: 3 - Alpha",
          "Intended Audience :: Developers",
          "License :: OSI Approved :: MIT License",
          "Programming Language :: Python :: 2.7",
          "Programming Language :: Python :: 3.3",
          "Programming Language :: Python :: 3.4",
      ])
