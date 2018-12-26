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
from http import HTTPStatus
from logging import DEBUG, Formatter, getLogger, INFO, StreamHandler
from tempfile import TemporaryDirectory

import yaml
from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf

from cockatrice import NAME
from cockatrice.index_http_server import IndexHTTPServer
from cockatrice.index_server import IndexServer


class TestIndexHTTPServer(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.example_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../example'))

        host = '0.0.0.0'
        port = 0
        peer_addrs = []
        dump_file = self.temp_dir.name + '/data.dump'
        http_port = 0

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

        self.index_server = IndexServer(host, port, peer_addrs, conf, index_dir, logger=logger)
        self.index_http_server = IndexHTTPServer(self.index_server, host, http_port, logger=logger,
                                                 http_logger=http_logger, metrics_registry=metrics_registry)

        self.test_client = self.index_http_server.get_test_client()

    def tearDown(self):
        self.index_server.stop()
        self.index_http_server.stop()
        self.temp_dir.cleanup()

    def test_metrics(self):
        response = self.test_client.get('/metrics')
        self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_root(self):
        response = self.test_client.get('/')
        self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_put_index(self):
        index_name = 'test_index'
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/' + index_name, data=schema_yaml, query_string='sync=True',
                                        headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

    def test_get_index(self):
        index_name = 'test_index'
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/' + index_name, data=schema_yaml, query_string='sync=True',
                                        headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # get index
        response = self.test_client.get('/indices/' + index_name)
        self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_delete_index(self):
        index_name = 'test_index'
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/' + index_name, data=schema_yaml, query_string='sync=True',
                                        headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # get index
        response = self.test_client.get('/indices/' + index_name)
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # delete index
        response = self.test_client.delete('/indices/' + index_name, query_string='sync=True')
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get index
        response = self.test_client.get('/indices/' + index_name)
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_put_document_yaml(self):
        # read schema
        index_name = 'test_index'
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/' + index_name, data=schema_yaml, query_string='sync=True',
                                        headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read document
        with open(self.example_dir + '/doc1.yaml', 'r', encoding='utf-8') as file_obj:
            doc = file_obj.read()

        # put document
        response = self.test_client.put('/indices/' + index_name + '/documents/1', data=doc,
                                        query_string='sync=True', headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

    def test_put_document_json(self):
        # read schema
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/test_index', data=schema_yaml, query_string='sync=True',
                                        headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read document
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            doc = file_obj.read()

        # put document
        response = self.test_client.put('/indices/test_index/documents/1', data=doc,
                                        query_string='sync=True', headers={'Content-Type': 'application/json'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

    def test_get_document_yaml(self):
        # read schema
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/test_index', data=schema_yaml, query_string='sync=True',
                                        headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read document
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            doc_json = file_obj.read()

        # put document
        response = self.test_client.put('/indices/test_index/documents/1', data=doc_json,
                                        query_string='sync=True', headers={'Content-Type': 'application/json'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # get document
        response = self.test_client.get('/indices/test_index/documents/1?output=yaml')
        self.assertEqual(HTTPStatus.OK, response.status_code)

        data = yaml.safe_load(response.data)
        expected_doc_id = '1'
        actual_doc_id = data['fields']['id']
        self.assertEqual(expected_doc_id, actual_doc_id)

    def test_get_document_json(self):
        # read schema
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/test_index', data=schema_yaml, query_string='sync=True',
                                        headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read document
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            doc_json = file_obj.read()

        # put document
        response = self.test_client.put('/indices/test_index/documents/1', data=doc_json,
                                        query_string='sync=True', headers={'Content-Type': 'application/json'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # get document
        response = self.test_client.get('/indices/test_index/documents/1', query_string='output=json')
        self.assertEqual(HTTPStatus.OK, response.status_code)

        data = json.loads(response.data)
        expected_doc_id = '1'
        actual_doc_id = data['fields']['id']
        self.assertEqual(expected_doc_id, actual_doc_id)

    def test_delete_document(self):
        # read schema
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/test_index', data=schema_yaml, query_string='sync=True',
                                        headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read document
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            doc_json = file_obj.read()

        # put document
        response = self.test_client.put('/indices/test_index/documents/1', data=doc_json, query_string='sync=True',
                                        headers={'Content-Type': 'application/json'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # delete document
        response = self.test_client.delete('/indices/test_index/documents/1', query_string='sync=True')
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get document
        response = self.test_client.get('/indices/test_index/documents/1')
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

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
