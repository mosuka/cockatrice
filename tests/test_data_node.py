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
import json

from logging import getLogger, StreamHandler, Formatter, DEBUG, INFO
from tempfile import TemporaryDirectory

from pysyncobj import SyncObjConf

from basilisk import APP_NAME
from basilisk.data_node import DataNode
from basilisk.schema import Schema


class TestDataNode(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.conf_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), "../conf"))
        self.example_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), "../example"))

        bind_addr = "0.0.0.0:0"
        peer_addrs = []
        dump_file = self.temp_dir.name + "/data.dump"

        index_dir = self.temp_dir.name + "/index"
        schema_file = self.conf_dir + "/schema.yaml"

        logger = getLogger(APP_NAME)
        log_handler = StreamHandler()
        logger.setLevel(DEBUG)
        log_handler.setLevel(INFO)
        log_format = Formatter('%(asctime)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s')
        log_handler.setFormatter(log_format)
        logger.addHandler(log_handler)

        conf = SyncObjConf(
            fullDumpFile=dump_file,
            logCompactionMinTime=300,
            dynamicMembershipChange=True
        )

        schema = Schema(schema_file)

        self.data_node = DataNode(bind_addr, peer_addrs, conf, index_dir, schema, logger=logger)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_index(self):
        example_file = self.example_dir + "/doc1.json"

        file_obj = open(example_file, 'r', encoding='utf-8')
        example_data = json.loads(file_obj.read(), encoding='utf-8')
        file_obj.close()

        test_doc_id = "1"
        test_fields = example_data
        sync = True

        self.data_node.index(test_doc_id, test_fields, sync=sync)

        page = self.data_node.get(test_doc_id)

        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

    def test_delete(self):
        example_file = self.example_dir + "/doc1.json"

        file_obj = open(example_file, 'r', encoding='utf-8')
        example_data = json.loads(file_obj.read(), encoding='utf-8')
        file_obj.close()

        test_doc_id = "1"
        test_fields = example_data
        sync = True

        self.data_node.index(test_doc_id, test_fields, sync=sync)

        page = self.data_node.get(test_doc_id)

        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        self.data_node.delete(test_doc_id, sync=sync)

        page = self.data_node.get(test_doc_id)

        expected_count = 0
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

    def test_bulk_index(self):
        example_file = self.example_dir + "/bulk_index.json"

        file_obj = open(example_file, 'r', encoding='utf-8')
        example_data = json.loads(file_obj.read(), encoding='utf-8')
        file_obj.close()

        sync = True

        self.data_node.bulk_index(example_data, sync=sync)

        page = self.data_node.get("1")
        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get("2")
        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get("3")
        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get("4")
        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get("5")
        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

    def test_bulk_delete(self):
        example_file = self.example_dir + "/bulk_index.json"

        file_obj = open(example_file, 'r', encoding='utf-8')
        example_data = json.loads(file_obj.read(), encoding='utf-8')
        file_obj.close()

        sync = True

        self.data_node.bulk_index(example_data, sync=sync)

        example_file = self.example_dir + "/bulk_delete.json"

        file_obj = open(example_file, 'r', encoding='utf-8')
        example_data = json.loads(file_obj.read(), encoding='utf-8')
        file_obj.close()

        sync = True

        self.data_node.bulk_delete(example_data, sync=sync)

        page = self.data_node.get("1")
        expected_count = 0
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get("2")
        expected_count = 0
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get("3")
        expected_count = 0
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get("4")
        expected_count = 0
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get("5")
        expected_count = 0
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

    def test_get(self):
        example_file = self.example_dir + "/doc1.json"

        file_obj = open(example_file, 'r', encoding='utf-8')
        example_data = json.loads(file_obj.read(), encoding='utf-8')
        file_obj.close()

        test_doc_id = "1"
        test_fields = example_data
        sync = True

        self.data_node.index(test_doc_id, test_fields, sync=sync)

        page = self.data_node.get(test_doc_id)

        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

    def test_search(self):
        example_file = self.example_dir + "/bulk_index.json"

        file_obj = open(example_file, 'r', encoding='utf-8')
        example_data = json.loads(file_obj.read(), encoding='utf-8')
        file_obj.close()

        sync = True

        self.data_node.bulk_index(example_data, sync=sync)

        page = self.data_node.search("search", default_field="text", page_num=1, page_len=10)
        expected_count = 5
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.search("search engine", default_field="text", page_num=1, page_len=10)
        expected_count = 3
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.search("distributed search", default_field="text", page_num=1, page_len=10)
        expected_count = 2
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.search("web search", default_field="text", page_num=1, page_len=10)
        expected_count = 4
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)
