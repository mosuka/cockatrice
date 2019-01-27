# -*- coding: utf-8 -*-

# Copyright (c) 2019 Minoru Osuka
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

from cockatrice.index_config import IndexConfig


class TestIndexConfig(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.index_dir = self.temp_dir.name + '/index'
        self.example_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../example'))

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_create_from_yaml(self):
        file_path = self.example_dir + '/index_config.yaml'
        with open(file_path, 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())

        index_config = IndexConfig(index_config_dict)

        self.assertIsNotNone(index_config)

    def test_create_from_json(self):
        file_path = self.example_dir + '/index_config.json'
        with open(file_path, 'r', encoding='utf-8') as file_obj:
            index_config_dict = json.loads(file_obj.read())

        index_config = IndexConfig(index_config_dict)

        self.assertIsNotNone(index_config)

    def test_yaml_get_unique_field(self):
        file_path = self.example_dir + '/index_config.yaml'
        with open(file_path, 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())

        index_config = IndexConfig(index_config_dict)

        self.assertEqual('id', index_config.get_doc_id_field())

    def test_json_get_unique_field(self):
        file_path = self.example_dir + '/index_config.json'
        with open(file_path, 'r', encoding='utf-8') as file_obj:
            index_config_dict = json.loads(file_obj.read())

        index_config = IndexConfig(index_config_dict)

        self.assertEqual('id', index_config.get_doc_id_field())

    def test_yaml_get_writer_auto_commit_period(self):
        file_path = self.example_dir + '/index_config.yaml'
        with open(file_path, 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())

        index_config = IndexConfig(index_config_dict)

        self.assertEqual(30, index_config.get_writer_auto_commit_period())

    def test_json_get_writer_auto_commit_period(self):
        file_path = self.example_dir + '/index_config.json'
        with open(file_path, 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())

        index_config = IndexConfig(index_config_dict)

        self.assertEqual(30, index_config.get_writer_auto_commit_period())

    def test_yaml_get_writer_auto_commit_limit(self):
        file_path = self.example_dir + '/index_config.yaml'
        with open(file_path, 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())

        index_config = IndexConfig(index_config_dict)

        self.assertEqual(100, index_config.get_writer_auto_commit_limit())

    def test_json_get_writer_auto_commit_limit(self):
        file_path = self.example_dir + '/index_config.json'
        with open(file_path, 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())

        index_config = IndexConfig(index_config_dict)

        self.assertEqual(100, index_config.get_writer_auto_commit_limit())

    def test_yaml_get_writer_processors(self):
        file_path = self.example_dir + '/index_config.yaml'
        with open(file_path, 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())

        index_config = IndexConfig(index_config_dict)

        self.assertEqual(1, index_config.get_writer_processors())

    def test_json_get_writer_processors(self):
        file_path = self.example_dir + '/index_config.json'
        with open(file_path, 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())

        index_config = IndexConfig(index_config_dict)

        self.assertEqual(1, index_config.get_writer_processors())

    def test_yaml_get_writer_batch_size(self):
        file_path = self.example_dir + '/index_config.yaml'
        with open(file_path, 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())

        index_config = IndexConfig(index_config_dict)

        self.assertEqual(100, index_config.get_writer_batch_size())

    def test_json_get_writer_batch_size(self):
        file_path = self.example_dir + '/index_config.json'
        with open(file_path, 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())

        index_config = IndexConfig(index_config_dict)

        self.assertEqual(100, index_config.get_writer_batch_size())

    def test_yaml_get_writer_multi_segment(self):
        file_path = self.example_dir + '/index_config.yaml'
        with open(file_path, 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())

        index_config = IndexConfig(index_config_dict)

        self.assertTrue(index_config.get_writer_multi_segment())

    def test_json_get_writer_multi_segment(self):
        file_path = self.example_dir + '/index_config.json'
        with open(file_path, 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())

        index_config = IndexConfig(index_config_dict)

        self.assertTrue(index_config.get_writer_multi_segment())
