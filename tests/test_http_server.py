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

from basilisk import APP_NAME
from basilisk.data_node import DataNode
from basilisk.http_server import HTTPServer
from basilisk.schema import Schema


class TestKVSServer(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.conf_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../conf'))
        self.example_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../example'))

        http_port = 0
        bind_addr = '0.0.0.0:0'
        peer_addrs = []
        dump_file = self.temp_dir.name + '/data.dump'

        index_dir = self.temp_dir.name + '/index'
        schema_file = self.conf_dir + '/schema.yaml'

        logger = getLogger(APP_NAME)
        log_handler = StreamHandler()
        logger.setLevel(DEBUG)
        log_handler.setLevel(INFO)
        log_format = Formatter('%(asctime)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s')
        log_handler.setFormatter(log_format)
        logger.addHandler(log_handler)

        http_logger = getLogger(APP_NAME + '_http')
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

        schema = Schema(schema_file)

        data_node = DataNode(bind_addr, peer_addrs, conf, index_dir, schema, logger=logger)

        self.server = HTTPServer(APP_NAME, http_port, data_node, schema,
                                 logger=logger, http_logger=http_logger, metrics_registry=metrics_registry)

        self.client = self.server.get_test_client()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_metrics(self):
        response = self.client.get('/metrics')

        expected_status_code = HTTPStatus.OK
        actual_status_code = response.status_code

        self.assertEqual(expected_status_code, actual_status_code)

    def test_index(self):
        example_file = self.example_dir + '/doc1.json'

        file_obj = open(example_file, 'r', encoding='utf-8')
        example_data = json.loads(file_obj.read(), encoding='utf-8')
        file_obj.close()

        doc_id = '1'
        sync = True

        response = self.client.put('/rest/doc/' + doc_id, data=json.dumps(example_data),
                                   query_string='sync=' + str(sync))

        expected_status_code = HTTPStatus.CREATED
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        data = json.loads(response.data)

        expected_status_code = HTTPStatus.CREATED
        actual_status_code = data['status']['code']
        self.assertEqual(expected_status_code, actual_status_code)

    def test_delete(self):
        example_file = self.example_dir + '/doc1.json'

        file_obj = open(example_file, 'r', encoding='utf-8')
        example_data = json.loads(file_obj.read(), encoding='utf-8')
        file_obj.close()

        doc_id = '1'
        sync = True

        self.client.put('/rest/doc/' + doc_id, data=json.dumps(example_data),
                        query_string='sync=' + str(sync))

        response = self.client.delete('/rest/doc/' + doc_id, query_string='sync=' + str(sync))

        expected_status_code = HTTPStatus.OK
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        response = self.client.get('/rest/doc/' + doc_id)

        expected_status_code = HTTPStatus.NOT_FOUND
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

    def test_bulk_index(self):
        example_file = self.example_dir + '/bulk_index.json'

        file_obj = open(example_file, 'r', encoding='utf-8')
        example_data = json.loads(file_obj.read(), encoding='utf-8')
        file_obj.close()

        sync = True

        response = self.client.put('/rest/bulk', data=json.dumps(example_data), query_string='sync=' + str(sync))

        expected_status_code = HTTPStatus.CREATED
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        data = json.loads(response.data)

        expected_status_code = HTTPStatus.CREATED
        actual_status_code = data['status']['code']
        self.assertEqual(expected_status_code, actual_status_code)

        expected_count = 5
        actual_count = data['count']
        self.assertEqual(expected_count, actual_count)

    def test_bulk_delete(self):
        example_bulk_index = self.example_dir + '/bulk_index.json'

        file_obj = open(example_bulk_index, 'r', encoding='utf-8')
        example_data = json.loads(file_obj.read(), encoding='utf-8')
        file_obj.close()

        sync = True

        response = self.client.put('/rest/bulk', data=json.dumps(example_data), query_string='sync=' + str(sync))

        expected_status_code = HTTPStatus.CREATED
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        example_bulk_delete = self.example_dir + '/bulk_delete.json'

        file_obj = open(example_bulk_delete, 'r', encoding='utf-8')
        example_data = json.loads(file_obj.read(), encoding='utf-8')
        file_obj.close()

        sync = True

        response = self.client.delete('/rest/bulk', data=json.dumps(example_data), query_string='sync=' + str(sync))

        expected_status_code = HTTPStatus.OK
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        data = json.loads(response.data)

        expected_status_code = HTTPStatus.OK
        actual_status_code = data['status']['code']
        self.assertEqual(expected_status_code, actual_status_code)

        expected_count = 5
        actual_count = data['count']
        self.assertEqual(expected_count, actual_count)

    def test_get(self):
        example_file = self.example_dir + '/doc1.json'

        file_obj = open(example_file, 'r', encoding='utf-8')
        example_data = json.loads(file_obj.read(), encoding='utf-8')
        file_obj.close()

        test_doc_id = '1'
        test_fields = example_data
        sync = True

        self.client.put('/rest/doc/' + test_doc_id, data=json.dumps(test_fields),
                        query_string='sync=' + str(sync))

        response = self.client.get('/rest/doc/' + test_doc_id)

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

    def test_search(self):
        pass
