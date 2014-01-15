#!/usr/bin/env python
# vim: fileencoding=utf8:et:sw=4:ts=8:sts=4

import unittest

import cdb


class FailureTestCases(unittest.TestCase):
    def test_reuse_cdb_make(self):
        cm = cdb.cdbmake('data', 'tmp')
        cm.add('foo', 'bar')
        cm.finish()

        self.assertRaises(cdb.error, cm.add, 'spam', 'eggs')
        self.assertRaises(cdb.error, cm.addmany, [('spam', 'eggs')])
        self.assertRaises(cdb.error, cm.finish)



if __name__ == '__main__':
    unittest.main()
