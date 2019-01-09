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

import json
import os
import unittest
from tempfile import TemporaryDirectory

import yaml
from whoosh.index import create_in

from cockatrice.schema import Schema


class TestSchema(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.index_dir = self.temp_dir.name + '/index'
        self.example_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../example'))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_yaml(self):
        schema_file = self.example_dir + '/schema.yaml'
        with open(schema_file, 'r', encoding='utf-8') as file_obj:
            schema_dict = yaml.safe_load(file_obj.read())

        schema = Schema(schema_dict)

        self.assertIsNotNone(schema)

    def test_json(self):
        schema_file = self.example_dir + '/schema.json'
        with open(schema_file, 'r', encoding='utf-8') as file_obj:
            schema_dict = json.loads(file_obj.read())

        schema = Schema(schema_dict)

        self.assertIsNotNone(schema)

    def test_yaml_create_index(self):
        schema_file = self.example_dir + '/schema.yaml'
        with open(schema_file, 'r', encoding='utf-8') as file_obj:
            schema_dict = yaml.safe_load(file_obj.read())

        schema = Schema(schema_dict)

        if self.index_dir is not None:
            os.makedirs(self.index_dir, exist_ok=True)

        ix = create_in(self.index_dir, schema)

        self.assertIsNotNone(ix)

    def test_json_create_index(self):
        schema_file = self.example_dir + '/schema.json'
        with open(schema_file, 'r', encoding='utf-8') as file_obj:
            schema_dict = json.loads(file_obj.read())

        schema = Schema(schema_dict)

        if self.index_dir is not None:
            os.makedirs(self.index_dir, exist_ok=True)

        ix = create_in(self.index_dir, schema)

        self.assertIsNotNone(ix)

    def test_yaml_get_unique_field(self):
        schema_file = self.example_dir + '/schema.yaml'
        with open(schema_file, 'r', encoding='utf-8') as file_obj:
            schema_dict = yaml.safe_load(file_obj.read())

        schema = Schema(schema_dict)

        expected = 'id'
        actual = schema.get_doc_id_field()

        self.assertEqual(expected, actual)

    def test_json_get_unique_field(self):
        schema_file = self.example_dir + '/schema.json'
        with open(schema_file, 'r', encoding='utf-8') as file_obj:
            schema_dict = json.loads(file_obj.read())

        schema = Schema(schema_dict)

        expected = 'id'
        actual = schema.get_doc_id_field()

        self.assertEqual(expected, actual)

    def test_yaml_get_default_search_field(self):
        schema_file = self.example_dir + '/schema.yaml'
        with open(schema_file, 'r', encoding='utf-8') as file_obj:
            schema_dict = yaml.safe_load(file_obj.read())

        schema = Schema(schema_dict)

        expected = 'text'
        actual = schema.get_default_search_field()

        self.assertEqual(expected, actual)

    def test_json_get_default_search_field(self):
        schema_file = self.example_dir + '/schema.json'
        with open(schema_file, 'r', encoding='utf-8') as file_obj:
            schema_dict = json.loads(file_obj.read())

        schema = Schema(schema_dict)

        expected = 'text'
        actual = schema.get_default_search_field()

        self.assertEqual(expected, actual)
