#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import unittest
from smart_open import smart_open_lib


class Test(unittest.TestCase):


    def test_host_name(self):
        host = 'http://a.com/b'
        expected = 'http://a.com/b'
        self.assertTrue(expected == smart_open_lib._add_sheme_to_host(host))
        host = 'a.com/b'
        self.assertTrue(expected == smart_open_lib._add_sheme_to_host(host))
        host = 'https://a.com/b'
        expected = 'https://a.com/b'
        self.assertTrue(expected == smart_open_lib._add_sheme_to_host(host))
        host = 'httpa.com/b'
        expected = 'http://httpa.com/b'
        self.assertTrue(expected == smart_open_lib._add_sheme_to_host(host))

if __name__ == "__main__":
    unittest.main()