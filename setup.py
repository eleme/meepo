#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

# console_scripts
cmds = [
    "meventsourcing = meepo.apps.meventsourcing:main",
    "mnano = meepo.apps.mnano:main",
    "mprint = meepo.apps.mprint:main",
    "mreplicate = meepo.apps.mreplicate:main",
]


# requirements
install_requires = [
    "click>=2.4",
    "SQLAlchemy>=0.9.6",
]

mysqlbinlog_requires = [
    "mysql-replication>=0.2.0",
]

dev_requires = [
    "flake8>=2.2.1",
    "sphinx-rtd-theme>=0.1.6",
    "sphinx>=1.2.2",
] + mysqlbinlog_requires


setup(name="meepo",
      version="0.1.0",
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
          "mysqlbinlog": mysqlbinlog_requires,
          "dev": dev_requires,
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
