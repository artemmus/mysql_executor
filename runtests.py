#!/usr/bin/env python
"""
.. module:: runtests
.. moduleauthor:: Artem Mustafa <artemmus@yahoo.com>
"""

import unittest


def load_tests(loader, tests, pattern):
    return unittest.defaultTestLoader.discover('tests', pattern='test_*.py')


if __name__ == '__main__':
    unittest.main()