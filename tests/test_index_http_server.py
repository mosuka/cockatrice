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

from tempfile import TemporaryDirectory
from logging import getLogger, StreamHandler, Formatter, DEBUG, INFO
from http import HTTPStatus

from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf

from cockatrice import NAME
from cockatrice.index_server import IndexServer
from cockatrice.index_http_server import IndexHTTPServer


class TestIndexHTTPServer(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.conf_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../conf'))
        self.example_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../example'))

        http_port = 0
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

        http_logger = getLogger(NAME + '_http')
        http_log_handler = StreamHandler()
        http_logger.setLevel(INFO)
        http_log_handler.setLevel(INFO)
        http_log_format = Formatter('%(message)s')
        http_log_handler.setFormatter(http_log_format)
        http_logger.addHandler(http_log_handler)

        metrics_registry = CollectorRegistry()

        conf = SyncObjConf(
            fullDumpFile=dump_file,
            logCompactionMinTime=300,
            dynamicMembershipChange=True
        )

        index_server = IndexServer(bind_addr, peer_addrs, conf, index_dir, logger=logger)

        self.index_http_server = IndexHTTPServer(index_server, port=http_port,
                                                 logger=logger, http_logger=http_logger,
                                                 metrics_registry=metrics_registry)

        self.test_client = self.index_http_server.get_test_client()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_metrics(self):
        response = self.test_client.get('/metrics')

        expected_status_code = HTTPStatus.OK
        actual_status_code = response.status_code

        self.assertEqual(expected_status_code, actual_status_code)

    def test_root(self):
        response = self.test_client.get('/')
        expected_status_code = HTTPStatus.OK
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

    def test_create_index(self):
        index_name = 'test_index'
        with open(self.conf_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/' + index_name, data=schema_yaml, query_string='sync=True')
        expected_status_code = HTTPStatus.CREATED
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

    def test_get_index(self):
        index_name = 'test_index'
        with open(self.conf_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/' + index_name, data=schema_yaml, query_string='sync=True')
        expected_status_code = HTTPStatus.CREATED
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        # get index
        response = self.test_client.get('/indices/' + index_name)
        expected_status_code = HTTPStatus.OK
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

    def test_delete_index(self):
        index_name = 'test_index'
        with open(self.conf_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/' + index_name, data=schema_yaml, query_string='sync=True')
        expected_status_code = HTTPStatus.CREATED
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        # get index
        response = self.test_client.get('/indices/' + index_name)
        expected_status_code = HTTPStatus.OK
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        # delete index
        response = self.test_client.delete('/indices/' + index_name, query_string='sync=True')
        expected_status_code = HTTPStatus.OK
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        # get index
        response = self.test_client.get('/indices/' + index_name)
        expected_status_code = HTTPStatus.NOT_FOUND
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

    def test_index_document(self):
        index_name = 'test_index'
        with open(self.conf_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/' + index_name, data=schema_yaml, query_string='sync=True')
        expected_status_code = HTTPStatus.CREATED
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            doc_json = file_obj.read()

        doc_id = '1'

        response = self.test_client.put('/indices/' + index_name + '/documents/' + doc_id, data=json.dumps(doc_json),
                                        query_string='sync=True')

        expected_status_code = HTTPStatus.CREATED
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        data = json.loads(response.data)

        expected_status_code = HTTPStatus.CREATED
        actual_status_code = data['status']['code']
        self.assertEqual(expected_status_code, actual_status_code)

    def test_get_document(self):
        index_name = 'test_index'
        with open(self.conf_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/' + index_name, data=schema_yaml, query_string='sync=True')
        expected_status_code = HTTPStatus.CREATED
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            doc_json = file_obj.read()

        doc_id = '1'

        # index document
        response = self.test_client.put('/indices/' + index_name + '/documents/' + doc_id, data=doc_json,
                                        query_string='sync=True')
        expected_status_code = HTTPStatus.CREATED
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        # get document
        response = self.test_client.get('/indices/' + index_name + '/documents/' + doc_id)
        expected_status_code = HTTPStatus.OK
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        data = json.loads(response.data)

        expected_status_code = HTTPStatus.OK
        actual_status_code = data['status']['code']
        self.assertEqual(expected_status_code, actual_status_code)

        expected_doc_id = '1'
        actual_doc_id = data['doc']['fields']['id']
        self.assertEqual(expected_doc_id, actual_doc_id)

    def test_delete_document(self):
        index_name = 'test_index'
        with open(self.conf_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/' + index_name, data=schema_yaml, query_string='sync=True')
        expected_status_code = HTTPStatus.CREATED
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            doc_json = file_obj.read()

        doc_id = '1'

        # index document
        response = self.test_client.put('/indices/' + index_name + '/documents/' + doc_id, data=doc_json,
                                        query_string='sync=True')
        expected_status_code = HTTPStatus.CREATED
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        # delete document
        response = self.test_client.delete('/indices/' + index_name + '/documents/' + doc_id, query_string='sync=True')
        expected_status_code = HTTPStatus.OK
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        # get document
        response = self.test_client.get('/indices/' + index_name + '/documents/' + doc_id + doc_id)
        expected_status_code = HTTPStatus.NOT_FOUND
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

    # def test_bulk_index(self):
    #     example_file = self.example_dir + '/bulk_index.json'
    #
    #     file_obj = open(example_file, 'r', encoding='utf-8')
    #     example_data = json.loads(file_obj.read(), encoding='utf-8')
    #     file_obj.close()
    #
    #     sync = True
    #
    #     response = self.client.put('/rest/bulk', data=json.dumps(example_data), query_string='sync=' + str(sync))
    #
    #     expected_status_code = HTTPStatus.CREATED
    #     actual_status_code = response.status_code
    #     self.assertEqual(expected_status_code, actual_status_code)
    #
    #     data = json.loads(response.data)
    #
    #     expected_status_code = HTTPStatus.CREATED
    #     actual_status_code = data['status']['code']
    #     self.assertEqual(expected_status_code, actual_status_code)
    #
    #     expected_count = 5
    #     actual_count = data['count']
    #     self.assertEqual(expected_count, actual_count)
    #
    # def test_bulk_delete(self):
    #     example_bulk_index = self.example_dir + '/bulk_index.json'
    #
    #     file_obj = open(example_bulk_index, 'r', encoding='utf-8')
    #     example_data = json.loads(file_obj.read(), encoding='utf-8')
    #     file_obj.close()
    #
    #     sync = True
    #
    #     response = self.client.put('/rest/bulk', data=json.dumps(example_data), query_string='sync=' + str(sync))
    #
    #     expected_status_code = HTTPStatus.CREATED
    #     actual_status_code = response.status_code
    #     self.assertEqual(expected_status_code, actual_status_code)
    #
    #     example_bulk_delete = self.example_dir + '/bulk_delete.json'
    #
    #     file_obj = open(example_bulk_delete, 'r', encoding='utf-8')
    #     example_data = json.loads(file_obj.read(), encoding='utf-8')
    #     file_obj.close()
    #
    #     sync = True
    #
    #     response = self.client.delete_document('/rest/bulk', data=json.dumps(example_data), query_string='sync=' + str(sync))
    #
    #     expected_status_code = HTTPStatus.OK
    #     actual_status_code = response.status_code
    #     self.assertEqual(expected_status_code, actual_status_code)
    #
    #     data = json.loads(response.data)
    #
    #     expected_status_code = HTTPStatus.OK
    #     actual_status_code = data['status']['code']
    #     self.assertEqual(expected_status_code, actual_status_code)
    #
    #     expected_count = 5
    #     actual_count = data['count']
    #     self.assertEqual(expected_count, actual_count)
    #
    # def test_search(self):
    #     pass
