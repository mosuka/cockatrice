# -*- coding: utf-8 -*-

# Copyright (c) 2018 Minoru Osuka
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 		http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from tempfile import TemporaryDirectory

from basilisk.kvs import KeyValueStore


class TestKeyValueStore(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()

        bind_addr = "0.0.0.0:0"
        peer_addrs = []
        dump_file = self.temp_dir.name + "/data.dump"

        self.kvs = KeyValueStore(bind_addr, peer_addrs, dump_file=dump_file)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_put(self):
        test_key = "1"
        test_value = "aaa"
        sync = True

        self.kvs.set(test_key, test_value, sync)

        expected_value = "aaa"
        actual_value = self.kvs.get(test_key)

        self.assertEqual(expected_value, actual_value)

    def test_get(self):
        test_key = "2"
        test_value = "bbb"
        sync = True

        self.kvs.set(test_key, test_value, sync)

        expected_value = "bbb"
        actual_value = self.kvs.get(test_key)

        self.assertEqual(expected_value, actual_value)

    def test_delete(self):
        test_key = "3"
        test_value = "bbb"
        sync = True

        self.kvs.set(test_key, test_value, sync)

        self.kvs.delete(test_key, sync)

        expected_value = None
        actual_value = self.kvs.get(test_key)

        self.assertEqual(expected_value, actual_value)
