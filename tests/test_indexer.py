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
from logging import ERROR, Formatter, getLogger, INFO, NOTSET, StreamHandler
from tempfile import TemporaryDirectory
from time import sleep

import yaml
from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf
from whoosh.filedb.filestore import FileStorage

from cockatrice import NAME
from cockatrice.index_config import IndexConfig
from cockatrice.indexer import Indexer
from tests import get_free_port


class TestIndexer(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.example_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../example'))

        host = '0.0.0.0'
        port = get_free_port()
        seed_addr = None
        conf = SyncObjConf(
            fullDumpFile=self.temp_dir.name + '/index.zip',
            logCompactionMinTime=300,
            dynamicMembershipChange=True
        )
        data_dir = self.temp_dir.name + '/index'
        grpc_port = get_free_port()
        grpc_max_workers = 10
        http_port = get_free_port()
        logger = getLogger(NAME)
        log_handler = StreamHandler()
        logger.setLevel(ERROR)
        log_handler.setLevel(INFO)
        log_format = Formatter('%(asctime)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s')
        log_handler.setFormatter(log_format)
        logger.addHandler(log_handler)
        http_logger = getLogger(NAME + '_http')
        http_log_handler = StreamHandler()
        http_logger.setLevel(NOTSET)
        http_log_handler.setLevel(INFO)
        http_log_format = Formatter('%(message)s')
        http_log_handler.setFormatter(http_log_format)
        http_logger.addHandler(http_log_handler)
        metrics_registry = CollectorRegistry()

        self.indexer = Indexer(host=host, port=port, seed_addr=seed_addr, conf=conf, data_dir=data_dir,
                               grpc_port=grpc_port, grpc_max_workers=grpc_max_workers, http_port=http_port,
                               logger=logger, http_logger=http_logger, metrics_registry=metrics_registry)

    def tearDown(self):
        self.indexer.stop()
        self.temp_dir.cleanup()

    def test_create_index(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())
        index_config = IndexConfig(index_config_dict)

        # create index
        index_name = 'test_file_index'
        self.indexer.create_index(index_name, index_config, sync=True)
        self.assertTrue(self.indexer.is_index_exist(index_name))

    def test_delete_index(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())
        index_config = IndexConfig(index_config_dict)

        # create index
        index_name = 'test_file_index'
        self.indexer.create_index(index_name, index_config, sync=True)
        self.assertTrue(self.indexer.is_index_exist(index_name))

        # delete index
        self.indexer.delete_index(index_name, sync=True)
        self.assertFalse(self.indexer.is_index_exist(index_name))

    def test_get_index(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())
        index_config = IndexConfig(index_config_dict)

        # create index
        index_name = 'test_file_index'
        self.indexer.create_index(index_name, index_config, sync=True)
        self.assertTrue(self.indexer.is_index_exist(index_name))

        i = self.indexer.get_index(index_name)
        self.assertTrue(isinstance(i.storage, FileStorage))

        # # close index
        # self.index_core.close_index(index_name)

    def test_put_document(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())
        index_config = IndexConfig(index_config_dict)

        # create index
        index_name = 'test_file_index'
        self.indexer.create_index(index_name, index_config, sync=True)
        self.assertTrue(self.indexer.is_index_exist(index_name))

        test_doc_id = '1'
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            test_fields = json.loads(file_obj.read(), encoding='utf-8')

        # put document
        count = self.indexer.put_document(index_name, test_doc_id, test_fields, sync=True)
        self.assertEqual(1, count)

    def test_commit(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())
        index_config = IndexConfig(index_config_dict)

        # create index
        index_name = 'test_file_index'
        self.indexer.create_index(index_name, index_config, sync=True)
        self.assertTrue(self.indexer.is_index_exist(index_name))

        test_doc_id = '1'
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            test_fields = json.loads(file_obj.read(), encoding='utf-8')

        # put document
        count = self.indexer.put_document(index_name, test_doc_id, test_fields, sync=True)
        self.assertEqual(1, count)

        # commit
        success = self.indexer.commit_index(index_name, sync=True)
        self.assertTrue(success)

        # get document
        results_page = self.indexer.get_document(index_name, test_doc_id)
        self.assertEqual(1, results_page.total)

    def test_rollback(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())
        index_config = IndexConfig(index_config_dict)

        # create index
        index_name = 'test_file_index'
        self.indexer.create_index(index_name, index_config, sync=True)
        self.assertTrue(self.indexer.is_index_exist(index_name))

        test_doc_id = '1'
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            test_fields = json.loads(file_obj.read(), encoding='utf-8')

        # put document
        count = self.indexer.put_document(index_name, test_doc_id, test_fields, sync=True)
        self.assertEqual(1, count)

        # rollback
        success = self.indexer.rollback_index(index_name, sync=True)
        self.assertTrue(success)

        # # get document
        # results_page = self.index_core.get_document(index_name, test_doc_id)
        # self.assertEqual(0, results_page.total)

    def test_get_document(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())
        index_config = IndexConfig(index_config_dict)

        # create index
        index_name = 'test_file_index'
        self.indexer.create_index(index_name, index_config, sync=True)
        self.assertTrue(self.indexer.is_index_exist(index_name))

        test_doc_id = '1'
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            test_fields = json.loads(file_obj.read(), encoding='utf-8')

        # put document
        count = self.indexer.put_document(index_name, test_doc_id, test_fields, sync=True)
        self.assertEqual(1, count)

        # commit
        success = self.indexer.commit_index(index_name, sync=True)
        self.assertTrue(success)

        # get document
        results_page = self.indexer.get_document(index_name, test_doc_id)
        self.assertEqual(1, results_page.total)

    def test_delete_document(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())
        index_config = IndexConfig(index_config_dict)

        # create index
        index_name = 'test_file_index'
        self.indexer.create_index(index_name, index_config, sync=True)
        self.assertTrue(self.indexer.is_index_exist(index_name))

        test_doc_id = '1'
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            test_fields = json.loads(file_obj.read(), encoding='utf-8')

        # put document
        count = self.indexer.put_document(index_name, test_doc_id, test_fields, sync=True)
        self.assertEqual(1, count)

        # commit
        success = self.indexer.commit_index(index_name, sync=True)
        self.assertTrue(success)

        # get document
        results_page = self.indexer.get_document(index_name, test_doc_id)
        self.assertEqual(1, results_page.total)

        # delete document
        count = self.indexer.delete_document(index_name, test_doc_id, sync=True)
        self.assertEqual(1, count)

        # commit
        success = self.indexer.commit_index(index_name, sync=True)
        self.assertTrue(success)

        # get document
        results_page = self.indexer.get_document(index_name, test_doc_id)
        self.assertEqual(0, results_page.total)

    def test_put_documents(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())
        index_config = IndexConfig(index_config_dict)

        # create index
        index_name = 'test_file_index'
        self.indexer.create_index(index_name, index_config, sync=True)
        self.assertTrue(self.indexer.is_index_exist(index_name))

        with open(self.example_dir + '/bulk_put.json', 'r', encoding='utf-8') as file_obj:
            test_docs = json.loads(file_obj.read(), encoding='utf-8')

        # put documents in bulk
        count = self.indexer.put_documents(index_name, test_docs, sync=True)
        self.assertEqual(5, count)

        # commit
        success = self.indexer.commit_index(index_name, sync=True)
        self.assertTrue(success)

        results_page = self.indexer.get_document(index_name, '1')
        self.assertEqual(1, results_page.total)

        results_page = self.indexer.get_document(index_name, '2')
        self.assertEqual(1, results_page.total)

        results_page = self.indexer.get_document(index_name, '3')
        self.assertEqual(1, results_page.total)

        results_page = self.indexer.get_document(index_name, '4')
        self.assertEqual(1, results_page.total)

        results_page = self.indexer.get_document(index_name, '5')
        self.assertEqual(1, results_page.total)

    def test_delete_documents(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())
        index_config = IndexConfig(index_config_dict)

        # create index
        index_name = 'test_file_index'
        self.indexer.create_index(index_name, index_config, sync=True)
        self.assertTrue(self.indexer.is_index_exist(index_name))

        with open(self.example_dir + '/bulk_put.json', 'r', encoding='utf-8') as file_obj:
            test_docs = json.loads(file_obj.read(), encoding='utf-8')

        # put documents in bulk
        count = self.indexer.put_documents(index_name, test_docs, sync=True)
        self.assertEqual(5, count)

        # commit
        success = self.indexer.commit_index(index_name, sync=True)
        self.assertTrue(success)

        results_page = self.indexer.get_document(index_name, '1')
        self.assertEqual(1, results_page.total)

        results_page = self.indexer.get_document(index_name, '2')
        self.assertEqual(1, results_page.total)

        results_page = self.indexer.get_document(index_name, '3')
        self.assertEqual(1, results_page.total)

        results_page = self.indexer.get_document(index_name, '4')
        self.assertEqual(1, results_page.total)

        results_page = self.indexer.get_document(index_name, '5')
        self.assertEqual(1, results_page.total)

        with open(self.example_dir + '/bulk_delete.json', 'r', encoding='utf-8') as file_obj:
            test_docs = json.loads(file_obj.read(), encoding='utf-8')

        # delete documents in bulk
        count = self.indexer.delete_documents(index_name, test_docs, sync=True)
        self.assertEqual(5, count)

        # commit
        success = self.indexer.commit_index(index_name, sync=True)
        self.assertTrue(success)

        results_page = self.indexer.get_document(index_name, '1')
        self.assertEqual(0, results_page.total)

        results_page = self.indexer.get_document(index_name, '2')
        self.assertEqual(0, results_page.total)

        results_page = self.indexer.get_document(index_name, '3')
        self.assertEqual(0, results_page.total)

        results_page = self.indexer.get_document(index_name, '4')
        self.assertEqual(0, results_page.total)

        results_page = self.indexer.get_document(index_name, '5')
        self.assertEqual(0, results_page.total)

    def test_search_documents(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())
        index_config = IndexConfig(index_config_dict)

        # create file index
        index_name = 'test_file_index'
        self.indexer.create_index(index_name, index_config, sync=True)
        self.assertTrue(self.indexer.is_index_exist(index_name))

        # read documents
        with open(self.example_dir + '/bulk_put.json', 'r', encoding='utf-8') as file_obj:
            test_docs = json.loads(file_obj.read(), encoding='utf-8')

        # put documents in bulk
        count = self.indexer.put_documents(index_name, test_docs, sync=True)
        self.assertEqual(5, count)

        # commit
        success = self.indexer.commit_index(index_name, sync=True)
        self.assertTrue(success)

        # search documents
        page = self.indexer.search_documents(index_name, 'search', search_field='text', page_num=1, page_len=10)
        self.assertEqual(5, page.total)

    def test_snapshot_exists(self):
        # snapshot exists
        self.assertFalse(self.indexer.is_snapshot_exist())

        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())
        index_config = IndexConfig(index_config_dict)

        # create file index
        index_name = 'test_file_index'
        self.indexer.create_index(index_name, index_config, sync=True)
        self.assertTrue(self.indexer.is_index_exist(index_name))

        # read documents
        with open(self.example_dir + '/bulk_put.json', 'r', encoding='utf-8') as file_obj:
            test_docs = json.loads(file_obj.read(), encoding='utf-8')

        # put documents in bulk
        count = self.indexer.put_documents(index_name, test_docs, sync=True)
        self.assertEqual(5, count)

        # commit
        success = self.indexer.commit_index(index_name, sync=True)
        self.assertTrue(success)

        # search documents
        page = self.indexer.search_documents(index_name, 'search', search_field='text', page_num=1, page_len=10)
        self.assertEqual(5, page.total)

        # create snapshot
        self.indexer.create_snapshot(sync=True)
        sleep(1)  # wait for snapshot file to be created
        self.assertTrue(os.path.exists(self.indexer.get_snapshot_file_name()))

        with zipfile.ZipFile(self.indexer.get_snapshot_file_name()) as f:
            self.assertTrue('raft.bin' in f.namelist())
            self.assertTrue('test_file_index_WRITELOCK' in f.namelist())
            self.assertEqual(1,
                             len([n for n in f.namelist() if n.startswith('_test_file_index_') and n.endswith('.toc')]))
            self.assertEqual(1,
                             len([n for n in f.namelist() if n.startswith('test_file_index_') and n.endswith('.seg')]))

        # snapshot exists
        self.assertTrue(True, self.indexer.is_snapshot_exist())

    def test_create_snapshot(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())
        index_config = IndexConfig(index_config_dict)

        # create file index
        index_name = 'test_file_index'
        self.indexer.create_index(index_name, index_config, sync=True)
        self.assertTrue(self.indexer.is_index_exist(index_name))

        with open(self.example_dir + '/bulk_put.json', 'r', encoding='utf-8') as file_obj:
            test_docs = json.loads(file_obj.read(), encoding='utf-8')

        # put documents in bulk
        count = self.indexer.put_documents(index_name, test_docs, sync=True)
        self.assertEqual(5, count)

        # commit
        success = self.indexer.commit_index(index_name, sync=True)
        self.assertTrue(success)

        # search documents
        page = self.indexer.search_documents(index_name, 'search', search_field='text', page_num=1, page_len=10)
        self.assertEqual(5, page.total)

        # create snapshot
        self.indexer.create_snapshot(sync=True)
        sleep(5)  # wait for snapshot file to be created
        self.assertTrue(os.path.exists(self.indexer.get_snapshot_file_name()))

        with zipfile.ZipFile(self.indexer.get_snapshot_file_name()) as f:
            self.assertTrue('raft.bin' in f.namelist())
            self.assertTrue(
                0 < len([n for n in f.namelist() if n.startswith('_test_file_index_') and n.endswith('.toc')]))
            self.assertTrue(
                0 < len([n for n in f.namelist() if n.startswith('test_file_index_') and n.endswith('.seg')]))
            self.assertTrue('test_file_index_WRITELOCK' in f.namelist())
            self.assertTrue(self.indexer.get_index_config_file(index_name) in f.namelist())

    def test_create_snapshot_ram(self):
        # read index config
        with open(self.example_dir + '/index_config_ram.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_dict = yaml.safe_load(file_obj.read())
        index_config = IndexConfig(index_config_dict)

        # create file index
        index_name = 'test_file_index'
        self.indexer.create_index(index_name, index_config, sync=True)
        self.assertTrue(self.indexer.is_index_exist(index_name))

        with open(self.example_dir + '/bulk_put.json', 'r', encoding='utf-8') as file_obj:
            test_docs = json.loads(file_obj.read(), encoding='utf-8')

        # put documents in bulk
        count = self.indexer.put_documents(index_name, test_docs, sync=True)
        self.assertEqual(5, count)

        # commit
        success = self.indexer.commit_index(index_name, sync=True)
        self.assertTrue(success)

        # search documents
        page = self.indexer.search_documents(index_name, 'search', search_field='text', page_num=1, page_len=10)
        self.assertEqual(5, page.total)

        # create snapshot
        self.indexer.create_snapshot(sync=True)
        sleep(5)  # wait for snapshot file to be created
        self.assertTrue(os.path.exists(self.indexer.get_snapshot_file_name()))

        with zipfile.ZipFile(self.indexer.get_snapshot_file_name()) as f:
            self.assertTrue('raft.bin' in f.namelist())
            self.assertEqual(1,
                             len([n for n in f.namelist() if n.startswith('_test_file_index_') and n.endswith('.toc')]))
            self.assertEqual(1,
                             len([n for n in f.namelist() if n.startswith('test_file_index_') and n.endswith('.seg')]))
            self.assertTrue(self.indexer.get_index_config_file(index_name) in f.namelist())
