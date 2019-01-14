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
from http import HTTPStatus
from logging import ERROR, Formatter, getLogger, INFO, NOTSET, StreamHandler
from tempfile import TemporaryDirectory
from time import sleep

import yaml
from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf

from cockatrice import NAME
from cockatrice.index_core import IndexCore
from cockatrice.index_grpc_server import IndexGRPCServer
from cockatrice.index_http_server import IndexHTTPServer
from tests import get_free_port


class TestIndexHTTPServer(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.example_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../example'))

        host = '0.0.0.0'
        port = get_free_port()
        peer_addrs = []
        dump_file = self.temp_dir.name + '/snapshot.zip'

        grpc_port = get_free_port()
        grpc_max_workers = 10

        http_port = get_free_port()

        index_dir = self.temp_dir.name + '/index'

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

        conf = SyncObjConf(
            fullDumpFile=dump_file,
            logCompactionMinTime=300,
            dynamicMembershipChange=True
        )

        self.index_core = IndexCore(host=host, port=port, peer_addrs=peer_addrs, conf=conf, index_dir=index_dir,
                                    logger=logger, metrics_registry=metrics_registry)
        self.index_grpc_server = IndexGRPCServer(self.index_core, host=host, port=grpc_port,
                                                 max_workers=grpc_max_workers, logger=logger,
                                                 metrics_registry=metrics_registry)
        self.index_http_server = IndexHTTPServer(grpc_port, host, http_port, logger=logger, http_logger=http_logger,
                                                 metrics_registry=metrics_registry)

        self.test_client = self.index_http_server.get_test_client()

    def tearDown(self):
        self.index_core.stop()
        self.index_http_server.stop()
        self.temp_dir.cleanup()

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

        # read document 1
        with open(self.example_dir + '/doc1.yaml', 'r', encoding='utf-8') as file_obj:
            doc = file_obj.read()

        # put document 1
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

        # read document 1
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            doc = file_obj.read()

        # put document 1
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

        # read document 1
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            doc_json = file_obj.read()

        # put document 1
        response = self.test_client.put('/indices/test_index/documents/1', data=doc_json,
                                        query_string='sync=True', headers={'Content-Type': 'application/json'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # get document 1
        response = self.test_client.get('/indices/test_index/documents/1?output=yaml')
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = yaml.safe_load(response.data)
        self.assertEqual('1', data['fields']['id'])

    def test_get_document_json(self):
        # read schema
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/test_index', data=schema_yaml, query_string='sync=True',
                                        headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read document 1
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            doc_json = file_obj.read()

        # put document 1
        response = self.test_client.put('/indices/test_index/documents/1', data=doc_json,
                                        query_string='sync=True', headers={'Content-Type': 'application/json'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # get document 1
        response = self.test_client.get('/indices/test_index/documents/1', query_string='output=json')
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.data)
        self.assertEqual('1', data['fields']['id'])

    def test_delete_document(self):
        # read schema
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/test_index', data=schema_yaml, query_string='sync=True',
                                        headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read document 1
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            doc_json = file_obj.read()

        # put document 1
        response = self.test_client.put('/indices/test_index/documents/1', data=doc_json, query_string='sync=True',
                                        headers={'Content-Type': 'application/json'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # delete document 1
        response = self.test_client.delete('/indices/test_index/documents/1', query_string='sync=True')
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get document 1
        response = self.test_client.get('/indices/test_index/documents/1')
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_put_documents_json(self):
        # read schema
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/test_index', data=schema_yaml, query_string='sync=True',
                                        headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read documents
        with open(self.example_dir + '/bulk_put.json', 'r', encoding='utf-8') as file_obj:
            docs_json = file_obj.read()

        # put documents
        response = self.test_client.put('/indices/test_index/documents', data=docs_json, query_string='sync=True',
                                        headers={'Content-Type': 'application/json'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # get document 1
        response = self.test_client.get('/indices/test_index/documents/1', query_string='output=json')
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.data)
        self.assertEqual('1', data['fields']['id'])

        # get document 2
        response = self.test_client.get('/indices/test_index/documents/2', query_string='output=json')
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.data)
        self.assertEqual('2', data['fields']['id'])

        # get document 3
        response = self.test_client.get('/indices/test_index/documents/3', query_string='output=json')
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.data)
        self.assertEqual('3', data['fields']['id'])

        # get document 4
        response = self.test_client.get('/indices/test_index/documents/4', query_string='output=json')
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.data)
        self.assertEqual('4', data['fields']['id'])

        # get document 5
        response = self.test_client.get('/indices/test_index/documents/5', query_string='output=json')
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.data)
        self.assertEqual('5', data['fields']['id'])

    def test_delete_documents_json(self):
        # read schema
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/test_index', data=schema_yaml, query_string='sync=True',
                                        headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read documents
        with open(self.example_dir + '/bulk_put.json', 'r', encoding='utf-8') as file_obj:
            docs_json = file_obj.read()

        # put documents
        response = self.test_client.put('/indices/test_index/documents', data=docs_json, query_string='sync=True',
                                        headers={'Content-Type': 'application/json'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # get document 1
        response = self.test_client.get('/indices/test_index/documents/1', query_string='output=json')
        self.assertEqual(HTTPStatus.OK, response.status_code)

        data = json.loads(response.data)
        self.assertEqual('1', data['fields']['id'])

        # get document 2
        response = self.test_client.get('/indices/test_index/documents/2', query_string='output=json')
        self.assertEqual(HTTPStatus.OK, response.status_code)

        data = json.loads(response.data)
        self.assertEqual('2', data['fields']['id'])

        # get document 3
        response = self.test_client.get('/indices/test_index/documents/3', query_string='output=json')
        self.assertEqual(HTTPStatus.OK, response.status_code)

        data = json.loads(response.data)
        self.assertEqual('3', data['fields']['id'])

        # get document 4
        response = self.test_client.get('/indices/test_index/documents/4', query_string='output=json')
        self.assertEqual(HTTPStatus.OK, response.status_code)

        data = json.loads(response.data)
        self.assertEqual('4', data['fields']['id'])

        # get document 5
        response = self.test_client.get('/indices/test_index/documents/5', query_string='output=json')
        self.assertEqual(HTTPStatus.OK, response.status_code)

        data = json.loads(response.data)
        self.assertEqual('5', data['fields']['id'])

        # read documents
        with open(self.example_dir + '/bulk_delete.json', 'r', encoding='utf-8') as file_obj:
            doc_ids_json = file_obj.read()

        # put documents
        response = self.test_client.delete('/indices/test_index/documents', data=doc_ids_json, query_string='sync=True',
                                           headers={'Content-Type': 'application/json'})
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get document 1
        response = self.test_client.get('/indices/test_index/documents/1')
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

        # get document 2
        response = self.test_client.get('/indices/test_index/documents/2')
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

        # get document 3
        response = self.test_client.get('/indices/test_index/documents/3')
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

        # get document 4
        response = self.test_client.get('/indices/test_index/documents/4')
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

        # get document 5
        response = self.test_client.get('/indices/test_index/documents/5')
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_search_documents_json(self):
        # read schema
        with open(self.example_dir + '/schema.yaml', 'r', encoding='utf-8') as file_obj:
            schema_yaml = file_obj.read()

        # create index
        response = self.test_client.put('/indices/test_index', data=schema_yaml, query_string='sync=True',
                                        headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read documents
        with open(self.example_dir + '/bulk_put.json', 'r', encoding='utf-8') as file_obj:
            docs_json = file_obj.read()

        # put documents
        response = self.test_client.put('/indices/test_index/documents', data=docs_json, query_string='sync=True',
                                        headers={'Content-Type': 'application/json'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read weighting
        with open(self.example_dir + '/weighting.json', 'r', encoding='utf-8') as file_obj:
            weighting_json = file_obj.read()

        # search documents
        response = self.test_client.post('/indices/test_index/search', data=weighting_json,
                                         query_string='query=search&search_field=text&page_num=1&page_len=10',
                                         headers={'Content-Type': 'application/json'})
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.data)
        self.assertEqual(5, data['results']['total'])

    def test_put_node(self):
        # get status
        response = self.test_client.get('/status')
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.data)
        self.assertEqual(0, data['node_status']['partner_nodes_count'])

        port = get_free_port()

        # put node
        response = self.test_client.put('/nodes/localhost:{0}'.format(port))
        sleep(1)  # wait for node to be added
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get status
        response = self.test_client.get('/status')
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.data)
        self.assertEqual(1, data['node_status']['partner_nodes_count'])

    def test_delete_node(self):
        # get status
        response = self.test_client.get('/status')
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.data)
        self.assertEqual(0, data['node_status']['partner_nodes_count'])

        port = get_free_port()

        # put node
        response = self.test_client.put('/nodes/localhost:{0}'.format(port))
        sleep(1)  # wait for node to be added
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get status
        response = self.test_client.get('/status')
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.data)
        self.assertEqual(1, data['node_status']['partner_nodes_count'])

        # delete node
        response = self.test_client.delete('/nodes/localhost:{0}'.format(port))
        sleep(1)  # wait for node to be deleted
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get status
        response = self.test_client.get('/status')
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.data)
        self.assertEqual(0, data['node_status']['partner_nodes_count'])

    def test_create_snapshot(self):
        # create snapshot
        response = self.test_client.put('/snapshot')
        self.assertEqual(HTTPStatus.ACCEPTED, response.status_code)

    def test_get_snapshot(self):
        # create snapshot
        response = self.test_client.get('/snapshot')
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

        # create snapshot
        response = self.test_client.put('/snapshot')
        self.assertEqual(HTTPStatus.ACCEPTED, response.status_code)

        sleep(1)  # wait for snapshot to be created

        # get snapshot
        response = self.test_client.get('/snapshot')
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # save snapshot
        download_file_name = self.temp_dir.name + '/snapshot_downloaded.zip'
        with open(download_file_name, 'wb') as f:
            f.write(response.data)

        # read snapshot
        with zipfile.ZipFile(download_file_name) as f:
            self.assertEqual(['raft.bin'], f.namelist())

    def test_is_healthy(self):
        # healthiness
        response = self.test_client.get('/healthiness')
        self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_is_alive(self):
        # liveness
        response = self.test_client.get('/liveness')
        self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_is_ready(self):
        # readiness
        response = self.test_client.get('/readiness')
        self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_get_status(self):
        # get node
        response = self.test_client.get('/status')
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.data)
        self.assertEqual(0, data['node_status']['partner_nodes_count'])

    def test_metrics(self):
        # metrics
        response = self.test_client.get('/metrics')
        self.assertEqual(HTTPStatus.OK, response.status_code)
