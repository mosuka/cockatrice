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
from whoosh.filedb.filestore import RamStorage, FileStorage
from prometheus_client.core import CollectorRegistry

from cockatrice import NAME
from cockatrice.index_server import IndexServer
from cockatrice.schema import Schema


class TestIndexServer(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.conf_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../conf'))
        self.example_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../example'))

        bind_addr = '0.0.0.0:0'
        peer_addrs = []
        dump_file = self.temp_dir.name + '/data.dump'

        index_dir = self.temp_dir.name + '/index'

        logger = getLogger(NAME)
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

        # metrics_registry = CollectorRegistry()

        self.data_node = IndexServer(bind_addr, peer_addrs, conf, index_dir, logger=logger)

    def tearDown(self):
        self.data_node.destroy()
        self.temp_dir.cleanup()

    def test_create_index(self):
        with open(self.conf_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create ram index
        index_name = 'test_ram_index'
        schema = Schema(schema_yaml)
        index = self.data_node.create_index(index_name, schema, use_ram_storage=True, sync=True)
        self.assertTrue(isinstance(index.storage, RamStorage))
        self.assertFalse(isinstance(index.storage, FileStorage))

        # create ram index
        index_name = 'test2_ram_index'
        schema = Schema(schema_yaml)
        index = self.data_node.create_index(index_name, schema, use_ram_storage=True, sync=True)
        self.assertTrue(isinstance(index.storage, RamStorage))
        self.assertFalse(isinstance(index.storage, FileStorage))

        # check the number of ram indices
        expected_file_count = 2
        actual_file_count = len(self.data_node.get_ram_storage().list())
        self.assertEqual(expected_file_count, actual_file_count)

        # create file index
        index_name = 'test_file_index'
        schema = Schema(schema_yaml)
        index = self.data_node.create_index(index_name, schema, use_ram_storage=False, sync=True)
        self.assertFalse(isinstance(index.storage, RamStorage))
        self.assertTrue(isinstance(index.storage, FileStorage))

        # create file index
        index_name = 'test2_file_index'
        schema = Schema(schema_yaml)
        index = self.data_node.create_index(index_name, schema, use_ram_storage=False, sync=True)
        self.assertFalse(isinstance(index.storage, RamStorage))
        self.assertTrue(isinstance(index.storage, FileStorage))

        # check the number of file file indices
        expected_file_count = 2
        actual_file_count = len(self.data_node.get_file_storage().list())
        self.assertEqual(expected_file_count, actual_file_count)

    def test_delete_index(self):
        with open(self.conf_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create ram index
        index_name = 'test_ram_index'
        schema = Schema(schema_yaml)
        index = self.data_node.create_index(index_name, schema, use_ram_storage=True, sync=True)
        self.assertTrue(isinstance(index.storage, RamStorage))
        self.assertFalse(isinstance(index.storage, FileStorage))

        # create ram index
        index_name = 'test2_ram_index'
        schema = Schema(schema_yaml)
        index = self.data_node.create_index(index_name, schema, use_ram_storage=True, sync=True)
        self.assertTrue(isinstance(index.storage, RamStorage))
        self.assertFalse(isinstance(index.storage, FileStorage))

        # check the number of ram indices
        expected_file_count = 2
        actual_file_count = len(self.data_node.get_ram_storage().list())
        self.assertEqual(expected_file_count, actual_file_count)

        # delete ram index
        self.data_node.delete_index(index_name, sync=True)
        expected_file_count = 1
        actual_file_count = len(self.data_node.get_ram_storage().list())
        self.assertEqual(expected_file_count, actual_file_count)

        # create file index
        index_name = 'test_file_index'
        schema = Schema(schema_yaml)
        index = self.data_node.create_index(index_name, schema, use_ram_storage=False, sync=True)
        self.assertFalse(isinstance(index.storage, RamStorage))
        self.assertTrue(isinstance(index.storage, FileStorage))

        # create file index
        index_name = 'test2_file_index'
        schema = Schema(schema_yaml)
        index = self.data_node.create_index(index_name, schema, use_ram_storage=False, sync=True)
        self.assertFalse(isinstance(index.storage, RamStorage))
        self.assertTrue(isinstance(index.storage, FileStorage))

        expected_file_count = 2
        actual_file_count = len(self.data_node.get_file_storage().list())
        self.assertEqual(expected_file_count, actual_file_count)

        self.data_node.delete_index(index_name, sync=True)
        expected_file_count = 1
        actual_file_count = len(self.data_node.get_file_storage().list())
        self.assertEqual(expected_file_count, actual_file_count)

    def test_get_index(self):
        with open(self.conf_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create ram index
        index_name = 'test_ram_index'
        schema = Schema(schema_yaml)
        index = self.data_node.create_index(index_name, schema, use_ram_storage=True, sync=True)
        self.assertTrue(isinstance(index.storage, RamStorage))
        self.assertFalse(isinstance(index.storage, FileStorage))

        i = self.data_node.get_index(index_name)
        self.assertTrue(isinstance(i.storage, RamStorage))
        self.assertFalse(isinstance(i.storage, FileStorage))

        # create file index
        index_name = 'test_file_index'
        schema = Schema(schema_yaml)
        index = self.data_node.create_index(index_name, schema, use_ram_storage=False, sync=True)
        self.assertFalse(isinstance(index.storage, RamStorage))
        self.assertTrue(isinstance(index.storage, FileStorage))

        i = self.data_node.get_index(index_name)
        self.assertFalse(isinstance(i.storage, RamStorage))
        self.assertTrue(isinstance(i.storage, FileStorage))

    def test_index_document(self):
        with open(self.conf_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create file index
        index_name = 'test_file_index'
        schema = Schema(schema_yaml)
        self.data_node.create_index(index_name, schema, use_ram_storage=False, sync=True)

        test_doc_id = '1'
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            test_fields = json.loads(file_obj.read(), encoding='utf-8')

        # index document
        self.data_node.index_document(index_name, test_doc_id, test_fields, sync=True)

        # get document
        page = self.data_node.get_document(index_name, test_doc_id)

        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

    def test_delete(self):
        with open(self.conf_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create file index
        index_name = 'test_file_index'
        schema = Schema(schema_yaml)
        self.data_node.create_index(index_name, schema, use_ram_storage=False, sync=True)

        test_doc_id = '1'
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            test_fields = json.loads(file_obj.read(), encoding='utf-8')

        # index document
        self.data_node.index_document(index_name, test_doc_id, test_fields, sync=True)

        # get document
        page = self.data_node.get_document(index_name, test_doc_id)
        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        # delete document
        self.data_node.delete_document(index_name, test_doc_id, sync=True)

        # get document
        page = self.data_node.get_document(index_name, test_doc_id)
        expected_count = 0
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

    def test_index_documents(self):
        with open(self.conf_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create file index
        index_name = 'test_file_index'
        schema = Schema(schema_yaml)
        self.data_node.create_index(index_name, schema, use_ram_storage=False, sync=True)

        with open(self.example_dir + '/bulk_index.json', 'r', encoding='utf-8') as file_obj:
            test_docs = json.loads(file_obj.read(), encoding='utf-8')

        # index documents in bulk
        self.data_node.index_documents(index_name, test_docs, sync=True)

        page = self.data_node.get_document(index_name, '1')
        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get_document(index_name, '2')
        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get_document(index_name, '3')
        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get_document(index_name, '4')
        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get_document(index_name, '5')
        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

    def test_delete_documents(self):
        with open(self.conf_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create file index
        index_name = 'test_file_index'
        schema = Schema(schema_yaml)
        self.data_node.create_index(index_name, schema, use_ram_storage=False, sync=True)

        with open(self.example_dir + '/bulk_index.json', 'r', encoding='utf-8') as file_obj:
            test_docs = json.loads(file_obj.read(), encoding='utf-8')

        # index documents in bulk
        self.data_node.index_documents(index_name, test_docs, sync=True)

        page = self.data_node.get_document(index_name, '1')
        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get_document(index_name, '2')
        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get_document(index_name, '3')
        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get_document(index_name, '4')
        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get_document(index_name, '5')
        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        with open(self.example_dir + '/bulk_delete.json', 'r', encoding='utf-8') as file_obj:
            test_docs = json.loads(file_obj.read(), encoding='utf-8')

        # index documents in bulk
        self.data_node.delete_documents(index_name, test_docs, sync=True)

        page = self.data_node.get_document(index_name, '1')
        expected_count = 0
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get_document(index_name, '2')
        expected_count = 0
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get_document(index_name, '3')
        expected_count = 0
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get_document(index_name, '4')
        expected_count = 0
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.get_document(index_name, '5')
        expected_count = 0
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

    def test_get_document(self):
        with open(self.conf_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create file index
        index_name = 'test_file_index'
        schema = Schema(schema_yaml)
        self.data_node.create_index(index_name, schema, use_ram_storage=False, sync=True)

        test_doc_id = '1'
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            test_fields = json.loads(file_obj.read(), encoding='utf-8')

        # index document
        self.data_node.index_document(index_name, test_doc_id, test_fields, sync=True)

        # get document
        page = self.data_node.get_document(index_name, test_doc_id)

        expected_count = 1
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

    def test_search_documents(self):
        with open(self.conf_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create file index
        index_name = 'test_file_index'
        schema = Schema(schema_yaml)
        self.data_node.create_index(index_name, schema, use_ram_storage=False, sync=True)

        with open(self.example_dir + '/bulk_index.json', 'r', encoding='utf-8') as file_obj:
            test_docs = json.loads(file_obj.read(), encoding='utf-8')

        # index documents in bulk
        self.data_node.index_documents(index_name, test_docs, sync=True)

        # search documents
        page = self.data_node.search_documents(index_name, 'search', search_field='text', page_num=1, page_len=10)
        expected_count = 5
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.search_documents(index_name, 'search engine', search_field='text', page_num=1,
                                               page_len=10)
        expected_count = 3
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.search_documents(index_name, 'distributed search', search_field='text', page_num=1,
                                               page_len=10)
        expected_count = 2
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        page = self.data_node.search_documents(index_name, 'web search', search_field='text', page_num=1, page_len=10)
        expected_count = 4
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)
