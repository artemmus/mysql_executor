#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import os.path
import re

with open(os.path.join(os.path.dirname(__file__),
                       'mysql_executor', '__init__.py')) as v_file:
    VERSION = re.compile(r".*__version__\s*=\s*'(.*?)'", re.S)\
                .match(v_file.read()).group(1)

setup(
    name='mysql_executor',
    version=VERSION,
    description='Non-blocking version of mysql-connector-python for asyncio',
    keywords='mysql asyncio non-blocking',
    author='Artem Mustafa',
    author_email='artemmus@yahoo.com',
    url='https://github.com/artemmus/mysql_executor',
    long_description=open('README.rst').read(),
    packages=['mysql_executor'],
    include_package_data=True,
    platforms=('Any',),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'License :: Freeware',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)