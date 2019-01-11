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
import zipfile
from logging import ERROR, Formatter, getLogger, INFO, StreamHandler
from tempfile import TemporaryDirectory
from time import sleep

import yaml
from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf
from whoosh.filedb.filestore import FileStorage

import cockatrice
from cockatrice.index_core import IndexCore
from cockatrice.schema import Schema
from tests import get_free_port


class TestIndexCore(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.example_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../example'))

        host = '0.0.0.0'
        port = get_free_port()
        peer_addrs = []
        dump_file = self.temp_dir.name + '/data.dump'

        index_dir = self.temp_dir.name + '/index'

        logger = getLogger(cockatrice.NAME)
        log_handler = StreamHandler()
        logger.setLevel(ERROR)
        log_handler.setLevel(INFO)
        log_format = Formatter('%(asctime)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s')
        log_handler.setFormatter(log_format)
        logger.addHandler(log_handler)

        metrics_registry = CollectorRegistry()

        conf = SyncObjConf(
            fullDumpFile=dump_file,
            logCompactionMinTime=300,
            dynamicMembershipChange=True
        )

        self.index_server = IndexCore(host=host, port=port, peer_addrs=peer_addrs, conf=conf, index_dir=index_dir,
                                      logger=logger, metrics_registry=metrics_registry)

    def tearDown(self):
        self.index_server.stop()
        self.temp_dir.cleanup()

    def test_create_index_from_yaml(self):
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            __dict = yaml.safe_load(file_obj.read())
        schema = Schema(__dict)

        # create index
        index_name = 'test_file_index'
        index = self.index_server.create_index(index_name, schema, sync=True)
        self.assertTrue(isinstance(index.storage, FileStorage))

        # create index
        index_name = 'test2_file_index'
        index = self.index_server.create_index(index_name, schema, sync=True)
        self.assertTrue(isinstance(index.storage, FileStorage))

        # check the number of file file indices
        expected_file_count = 2
        actual_file_count = len(self.index_server.get_file_storage().list())
        self.assertEqual(expected_file_count, actual_file_count)

    def test_create_index_from_json(self):
        with open(self.example_dir + '/schema.json', 'r', encoding='utf-8') as file_obj:
            __dict = json.loads(file_obj.read())
        schema = Schema(__dict)

        # create index
        index_name = 'test_file_index'
        index = self.index_server.create_index(index_name, schema, sync=True)
        self.assertTrue(isinstance(index.storage, FileStorage))

        # create index
        index_name = 'test2_file_index'
        index = self.index_server.create_index(index_name, schema, sync=True)
        self.assertTrue(isinstance(index.storage, FileStorage))

        # check the number of file file indices
        expected_file_count = 2
        actual_file_count = len(self.index_server.get_file_storage().list())
        self.assertEqual(expected_file_count, actual_file_count)

    def test_delete_index(self):
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            __dict = yaml.safe_load(file_obj.read())
        schema = Schema(__dict)

        # create index
        index_name = 'test_file_index'
        index = self.index_server.create_index(index_name, schema, sync=True)
        self.assertTrue(isinstance(index.storage, FileStorage))

        # create index
        index_name = 'test2_file_index'
        index = self.index_server.create_index(index_name, schema, sync=True)
        self.assertTrue(isinstance(index.storage, FileStorage))

        expected_file_count = 2
        actual_file_count = len(self.index_server.get_file_storage().list())
        self.assertEqual(expected_file_count, actual_file_count)

        self.index_server.delete_index(index_name, sync=True)
        expected_file_count = 1
        actual_file_count = len(self.index_server.get_file_storage().list())
        self.assertEqual(expected_file_count, actual_file_count)

    def test_get_index(self):
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            __dict = yaml.safe_load(file_obj.read())
        schema = Schema(__dict)

        # create index
        index_name = 'test_file_index'
        index = self.index_server.create_index(index_name, schema, sync=True)
        self.assertTrue(isinstance(index.storage, FileStorage))

        i = self.index_server.get_index(index_name)
        self.assertTrue(isinstance(i.storage, FileStorage))

    def test_put_document(self):
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            __dict = yaml.safe_load(file_obj.read())
        schema = Schema(__dict)

        # create index
        index_name = 'test_file_index'
        self.index_server.create_index(index_name, schema, sync=True)

        test_doc_id = '1'
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            test_fields = json.loads(file_obj.read(), encoding='utf-8')

        # index document
        self.index_server.put_document(index_name, test_doc_id, test_fields, sync=True)

        # get document
        results_page = self.index_server.get_document(index_name, test_doc_id)
        expected_count = 1
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

    def test_get_document(self):
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            __dict = yaml.safe_load(file_obj.read())
        schema = Schema(__dict)

        # create index
        index_name = 'test_file_index'
        self.index_server.create_index(index_name, schema, sync=True)

        test_doc_id = '1'
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            test_fields = json.loads(file_obj.read(), encoding='utf-8')

        # index document
        self.index_server.put_document(index_name, test_doc_id, test_fields, sync=True)

        # get document
        results_page = self.index_server.get_document(index_name, test_doc_id)
        expected_count = 1
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

    def test_delete_document(self):
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            __dict = yaml.safe_load(file_obj.read())
        schema = Schema(__dict)

        # create index
        index_name = 'test_file_index'
        self.index_server.create_index(index_name, schema, sync=True)

        test_doc_id = '1'
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            test_fields = json.loads(file_obj.read(), encoding='utf-8')

        # index document
        self.index_server.put_document(index_name, test_doc_id, test_fields, sync=True)

        # get document
        results_page = self.index_server.get_document(index_name, test_doc_id)
        expected_count = 1
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

        # delete document
        self.index_server.delete_document(index_name, test_doc_id, sync=True)

        # get document
        results_page = self.index_server.get_document(index_name, test_doc_id)
        expected_count = 0
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

    def test_put_documents(self):
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            __dict = yaml.safe_load(file_obj.read())
        schema = Schema(__dict)

        # create index
        index_name = 'test_file_index'
        self.index_server.create_index(index_name, schema, sync=True)

        with open(self.example_dir + '/bulk_put.json', 'r', encoding='utf-8') as file_obj:
            test_docs = json.loads(file_obj.read(), encoding='utf-8')

        # index documents in bulk
        self.index_server.put_documents(index_name, test_docs, sync=True)

        results_page = self.index_server.get_document(index_name, '1')
        expected_count = 1
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

        results_page = self.index_server.get_document(index_name, '2')
        expected_count = 1
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

        results_page = self.index_server.get_document(index_name, '3')
        expected_count = 1
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

        results_page = self.index_server.get_document(index_name, '4')
        expected_count = 1
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

        results_page = self.index_server.get_document(index_name, '5')
        expected_count = 1
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

    def test_delete_documents(self):
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            __dict = yaml.safe_load(file_obj.read())
        schema = Schema(__dict)

        # create index
        index_name = 'test_file_index'
        self.index_server.create_index(index_name, schema, sync=True)

        with open(self.example_dir + '/bulk_put.json', 'r', encoding='utf-8') as file_obj:
            test_docs = json.loads(file_obj.read(), encoding='utf-8')

        # put documents in bulk
        self.index_server.put_documents(index_name, test_docs, sync=True)

        results_page = self.index_server.get_document(index_name, '1')
        expected_count = 1
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

        results_page = self.index_server.get_document(index_name, '2')
        expected_count = 1
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

        results_page = self.index_server.get_document(index_name, '3')
        expected_count = 1
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

        results_page = self.index_server.get_document(index_name, '4')
        expected_count = 1
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

        results_page = self.index_server.get_document(index_name, '5')
        expected_count = 1
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

        with open(self.example_dir + '/bulk_delete.json', 'r', encoding='utf-8') as file_obj:
            test_docs = json.loads(file_obj.read(), encoding='utf-8')

        # delete documents in bulk
        self.index_server.delete_documents(index_name, test_docs, sync=True)

        results_page = self.index_server.get_document(index_name, '1')
        expected_count = 0
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

        results_page = self.index_server.get_document(index_name, '2')
        expected_count = 0
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

        results_page = self.index_server.get_document(index_name, '3')
        expected_count = 0
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

        results_page = self.index_server.get_document(index_name, '4')
        expected_count = 0
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

        results_page = self.index_server.get_document(index_name, '5')
        expected_count = 0
        actual_count = results_page.total
        self.assertEqual(expected_count, actual_count)

    def test_search_documents(self):
        # read schema
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            __dict = yaml.safe_load(file_obj.read())
        schema = Schema(__dict)

        # create file index
        index_name = 'test_file_index'
        self.index_server.create_index(index_name, schema, sync=True)

        # read documents
        with open(self.example_dir + '/bulk_put.json', 'r', encoding='utf-8') as file_obj:
            test_docs = json.loads(file_obj.read(), encoding='utf-8')

        # put documents
        self.index_server.put_documents(index_name, test_docs, sync=True)

        # search documents
        page = self.index_server.search_documents(index_name, 'search', search_field='text', page_num=1, page_len=10)
        self.assertEqual(5, page.total)

        page = self.index_server.search_documents(index_name, 'search engine', search_field='text', page_num=1,
                                                  page_len=10)
        self.assertEqual(3, page.total)

        page = self.index_server.search_documents(index_name, 'distributed search', search_field='text', page_num=1,
                                                  page_len=10)
        self.assertEqual(2, page.total)

        page = self.index_server.search_documents(index_name, 'web search', search_field='text', page_num=1,
                                                  page_len=10)
        self.assertEqual(4, page.total)

    def test_snapshot_exists(self):
        # snapshot exists
        exists = self.index_server.snapshot_exists()
        self.assertEqual(False, exists)

        # read schema
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            __dict = yaml.safe_load(file_obj.read())
        schema = Schema(__dict)

        # create file index
        index_name = 'test_file_index'
        self.index_server.create_index(index_name, schema, sync=True)

        # read documents
        with open(self.example_dir + '/bulk_put.json', 'r', encoding='utf-8') as file_obj:
            test_docs = json.loads(file_obj.read(), encoding='utf-8')

        # index documents in bulk
        self.index_server.put_documents(index_name, test_docs, sync=True)

        # search documents
        page = self.index_server.search_documents(index_name, 'search', search_field='text', page_num=1, page_len=10)
        expected_count = 5
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        # create snapshot
        self.index_server.forceLogCompaction()
        sleep(1)  # wait for snapshot file to be created
        self.assertEqual(True, os.path.exists(self.index_server.get_snapshot_file_name()))

        with zipfile.ZipFile(self.index_server.get_snapshot_file_name()) as f:
            self.assertEqual(True, 'raft.bin' in f.namelist())
            self.assertEqual(True, 'test_file_index_WRITELOCK' in f.namelist())
            self.assertEqual(True, '_test_file_index_1.toc' in f.namelist())
            self.assertEqual(1,
                             len([n for n in f.namelist() if n.startswith('test_file_index_') and n.endswith('.seg')]))

        # snapshot exists
        exists = self.index_server.snapshot_exists()
        self.assertEqual(True, exists)

    def test_create_snapshot(self):
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            __dict = yaml.safe_load(file_obj.read())
        schema = Schema(__dict)

        # create file index
        index_name = 'test_file_index'
        self.index_server.create_index(index_name, schema, sync=True)

        with open(self.example_dir + '/bulk_put.json', 'r', encoding='utf-8') as file_obj:
            test_docs = json.loads(file_obj.read(), encoding='utf-8')

        # index documents in bulk
        self.index_server.put_documents(index_name, test_docs, sync=True)

        # search documents
        page = self.index_server.search_documents(index_name, 'search', search_field='text', page_num=1, page_len=10)
        expected_count = 5
        actual_count = page.total
        self.assertEqual(expected_count, actual_count)

        # create snapshot
        self.index_server.forceLogCompaction()

        sleep(1)  # wait for snapshot file to be created

        self.assertEqual(True, os.path.exists(self.index_server.get_snapshot_file_name()))

        with zipfile.ZipFile(self.index_server.get_snapshot_file_name()) as f:
            self.assertEqual(True, 'raft.bin' in f.namelist())
            self.assertEqual(True, 'test_file_index_WRITELOCK' in f.namelist())
            self.assertEqual(True, '_test_file_index_1.toc' in f.namelist())
            self.assertEqual(1,
                             len([n for n in f.namelist() if n.startswith('test_file_index_') and n.endswith('.seg')]))
