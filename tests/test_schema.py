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

import os

from tempfile import TemporaryDirectory

from whoosh.index import create_in

from cockatrice.schema import Schema


class TestSchema(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.conf_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../conf'))

        self.index_dir = self.temp_dir.name + '/index'
        schema_file = self.conf_dir + '/schema.yaml'

        with open(schema_file, 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        self.schema = Schema(schema_yaml)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_create_index(self):
        if self.index_dir is not None:
            os.makedirs(self.index_dir, exist_ok=True)

        ix = create_in(self.index_dir, self.schema)

        self.assertIsNotNone(ix)

    def test_get_unique_field(self):
        expected = 'id'
        actual = self.schema.get_unique_field()

        self.assertEqual(expected, actual)

    def test_get_default_search_field(self):
        expected = 'text'
        actual = self.schema.get_default_search_field()

        self.assertEqual(expected, actual)
