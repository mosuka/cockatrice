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

import requests
import yaml
from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf

from cockatrice import NAME
from cockatrice.indexer import Indexer
from tests import get_free_port


class TestIndexHTTPServicer(unittest.TestCase):
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

        self.host = host
        self.port = http_port

    def tearDown(self):
        self.indexer.stop()
        self.temp_dir.cleanup()

    def test_root(self):
        # get
        response = requests.get('http://{0}:{1}/'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_put_index(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_yaml = file_obj.read()

        # create index
        response = requests.put('http://{0}:{1}/indices/test_index?sync=True'.format(self.host, self.port),
                                data=index_config_yaml.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

    def test_get_index(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_yaml = file_obj.read()

        # create index
        response = requests.put('http://{0}:{1}/indices/test_index?sync=True'.format(self.host, self.port),
                                data=index_config_yaml.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # get index
        response = requests.get('http://{0}:{1}/indices/test_index'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_delete_index(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_yaml = file_obj.read()

        # create index
        response = requests.put('http://{0}:{1}/indices/test_index?sync=True'.format(self.host, self.port),
                                data=index_config_yaml.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # get index
        response = requests.get('http://{0}:{1}/indices/test_index'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # delete index
        response = requests.delete('http://{0}:{1}/indices/test_index?sync=True'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get index
        response = requests.get('http://{0}:{1}/indices/test_index'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_put_document_yaml(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_yaml = file_obj.read()

        # create index
        response = requests.put('http://{0}:{1}/indices/test_index?sync=True'.format(self.host, self.port),
                                data=index_config_yaml.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read document 1
        with open(self.example_dir + '/doc1.yaml', 'r', encoding='utf-8') as file_obj:
            doc = file_obj.read()

        # put document 1
        response = requests.put('http://{0}:{1}/indices/test_index/documents/1?sync=True'.format(self.host, self.port),
                                data=doc.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

    def test_put_document_json(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_yaml = file_obj.read()

        # create index
        response = requests.put('http://{0}:{1}/indices/test_index?sync=True'.format(self.host, self.port),
                                data=index_config_yaml.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read document 1
        with open(self.example_dir + '/doc1.json', 'r', encoding='utf-8') as file_obj:
            doc = file_obj.read()

        # put document 1
        response = requests.put('http://{0}:{1}/indices/test_index/documents/1?sync=True'.format(self.host, self.port),
                                data=doc.encode('utf-8'), headers={'Content-Type': 'application/json'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

    def test_get_document_yaml(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_yaml = file_obj.read()

        # create index
        response = requests.put('http://{0}:{1}/indices/test_index?sync=True'.format(self.host, self.port),
                                data=index_config_yaml.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read document 1
        with open(self.example_dir + '/doc1.yaml', 'r', encoding='utf-8') as file_obj:
            doc = file_obj.read()

        # put document 1
        response = requests.put('http://{0}:{1}/indices/test_index/documents/1?sync=True'.format(self.host, self.port),
                                data=doc.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # commit
        response = requests.get('http://{0}:{1}/indices/test_index/commit?sync=True'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get document 1
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/1?output=yaml'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = yaml.safe_load(response.text)
        self.assertEqual('1', data['fields']['id'])

    def test_get_document_json(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_yaml = file_obj.read()

        # create index
        response = requests.put('http://{0}:{1}/indices/test_index?sync=True'.format(self.host, self.port),
                                data=index_config_yaml.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read document 1
        with open(self.example_dir + '/doc1.yaml', 'r', encoding='utf-8') as file_obj:
            doc = file_obj.read()

        # put document 1
        response = requests.put('http://{0}:{1}/indices/test_index/documents/1?sync=True'.format(self.host, self.port),
                                data=doc.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # commit
        response = requests.get('http://{0}:{1}/indices/test_index/commit?sync=True'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get document 1
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/1?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual('1', data['fields']['id'])

    def test_delete_document(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_yaml = file_obj.read()

        # create index
        response = requests.put('http://{0}:{1}/indices/test_index?sync=True'.format(self.host, self.port),
                                data=index_config_yaml.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read document 1
        with open(self.example_dir + '/doc1.yaml', 'r', encoding='utf-8') as file_obj:
            doc = file_obj.read()

        # put document 1
        response = requests.put('http://{0}:{1}/indices/test_index/documents/1?sync=True'.format(self.host, self.port),
                                data=doc.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # commit
        response = requests.get('http://{0}:{1}/indices/test_index/commit?sync=True'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get document 1
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/1?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual('1', data['fields']['id'])

        # delete document 1
        response = requests.delete(
            'http://{0}:{1}/indices/test_index/documents/1?sync=True'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # commit
        response = requests.get('http://{0}:{1}/indices/test_index/commit?sync=True'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get document 1
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/1?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_put_documents_json(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_yaml = file_obj.read()

        # create index
        response = requests.put('http://{0}:{1}/indices/test_index?sync=True'.format(self.host, self.port),
                                data=index_config_yaml.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read documents
        with open(self.example_dir + '/bulk_put.json', 'r', encoding='utf-8') as file_obj:
            docs_json = file_obj.read()

        # put documents
        response = requests.put('http://{0}:{1}/indices/test_index/documents?sync=True'.format(self.host, self.port),
                                data=docs_json.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # commit
        response = requests.get('http://{0}:{1}/indices/test_index/commit?sync=True'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get document 1
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/1?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual('1', data['fields']['id'])

        # get document 2
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/2?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual('2', data['fields']['id'])

        # get document 3
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/3?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual('3', data['fields']['id'])

        # get document 4
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/4?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual('4', data['fields']['id'])

        # get document 5
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/5?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual('5', data['fields']['id'])

    def test_delete_documents_json(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_yaml = file_obj.read()

        # create index
        response = requests.put('http://{0}:{1}/indices/test_index?sync=True'.format(self.host, self.port),
                                data=index_config_yaml.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read documents
        with open(self.example_dir + '/bulk_put.json', 'r', encoding='utf-8') as file_obj:
            docs_json = file_obj.read()

        # put documents
        response = requests.put('http://{0}:{1}/indices/test_index/documents?sync=True'.format(self.host, self.port),
                                data=docs_json.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # commit
        response = requests.get('http://{0}:{1}/indices/test_index/commit?sync=True'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get document 1
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/1?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual('1', data['fields']['id'])

        # get document 2
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/2?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual('2', data['fields']['id'])

        # get document 3
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/3?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual('3', data['fields']['id'])

        # get document 4
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/4?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual('4', data['fields']['id'])

        # get document 5
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/5?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual('5', data['fields']['id'])

        # read documents
        with open(self.example_dir + '/bulk_delete.json', 'r', encoding='utf-8') as file_obj:
            doc_ids_json = file_obj.read()

        # delete documents
        response = requests.delete('http://{0}:{1}/indices/test_index/documents?sync=True'.format(self.host, self.port),
                                   data=doc_ids_json.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # commit
        response = requests.get('http://{0}:{1}/indices/test_index/commit?sync=True'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get document 1
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/1?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

        # get document 2
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/2?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

        # get document 3
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/3?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

        # get document 4
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/4?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

        # get document 5
        response = requests.get(
            'http://{0}:{1}/indices/test_index/documents/5?output=json'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

    def test_search_documents_json(self):
        # read index config
        with open(self.example_dir + '/index_config.yaml', 'r', encoding='utf-8') as file_obj:
            index_config_yaml = file_obj.read()

        # create index
        response = requests.put('http://{0}:{1}/indices/test_index?sync=True'.format(self.host, self.port),
                                data=index_config_yaml.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # read documents
        with open(self.example_dir + '/bulk_put.json', 'r', encoding='utf-8') as file_obj:
            docs_json = file_obj.read()

        # put documents
        response = requests.put('http://{0}:{1}/indices/test_index/documents?sync=True'.format(self.host, self.port),
                                data=docs_json.encode('utf-8'), headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # commit
        response = requests.get('http://{0}:{1}/indices/test_index/commit?sync=True'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # read weighting
        with open(self.example_dir + '/weighting.json', 'r', encoding='utf-8') as file_obj:
            weighting_json = file_obj.read()

        # search documents
        response = requests.post(
            'http://{0}:{1}/indices/test_index/search?query=search&search_field=text&page_num=1&page_len=10'.format(
                self.host, self.port),
            data=weighting_json.encode('utf-8'), headers={'Content-Type': 'application/json'})
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual(5, data['results']['total'])

    def test_put_node(self):
        # get status
        response = requests.get('http://{0}:{1}/status'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual(0, data['node_status']['partner_nodes_count'])

        port = get_free_port()

        # put node
        response = requests.put('http://{0}:{1}/nodes/localhost:{2}'.format(self.host, self.port, port))
        sleep(1)  # wait for node to be added
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get status
        response = requests.get('http://{0}:{1}/status'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual(1, data['node_status']['partner_nodes_count'])

    def test_delete_node(self):
        # get status
        response = requests.get('http://{0}:{1}/status'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual(0, data['node_status']['partner_nodes_count'])

        port = get_free_port()

        # put node
        response = requests.put('http://{0}:{1}/nodes/localhost:{2}'.format(self.host, self.port, port))
        sleep(1)  # wait for node to be added
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get status
        response = requests.get('http://{0}:{1}/status'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual(1, data['node_status']['partner_nodes_count'])

        # delete node
        response = requests.delete('http://{0}:{1}/nodes/localhost:{2}'.format(self.host, self.port, port))
        sleep(1)  # wait for node to be deleted
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get status
        response = requests.get('http://{0}:{1}/status'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual(0, data['node_status']['partner_nodes_count'])

    def test_create_snapshot(self):
        # get snapshot
        response = requests.get('http://{0}:{1}/snapshot'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

        # create snapshot
        response = requests.put('http://{0}:{1}/snapshot'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.ACCEPTED, response.status_code)

        sleep(1)

        # get snapshot
        response = requests.get('http://{0}:{1}/snapshot'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_get_snapshot(self):
        # get snapshot
        response = requests.get('http://{0}:{1}/snapshot'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

        # create snapshot
        response = requests.put('http://{0}:{1}/snapshot'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.ACCEPTED, response.status_code)

        sleep(1)

        # get snapshot
        response = requests.get('http://{0}:{1}/snapshot'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # save snapshot
        download_file_name = self.temp_dir.name + '/snapshot_downloaded.zip'
        with open(download_file_name, 'wb') as f:
            f.write(response.content)

        # read snapshot
        with zipfile.ZipFile(download_file_name) as f:
            self.assertEqual(['raft.bin'], f.namelist())

    def test_is_healthy(self):
        # healthiness
        response = requests.get('http://{0}:{1}/healthiness'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_is_alive(self):
        # liveness
        response = requests.get('http://{0}:{1}/liveness'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_is_ready(self):
        # readiness
        response = requests.get('http://{0}:{1}/readiness'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

    def test_get_status(self):
        # get status
        response = requests.get('http://{0}:{1}/status'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.text)
        self.assertEqual(0, data['node_status']['partner_nodes_count'])

    def test_metrics(self):
        # metrics
        response = requests.get('http://{0}:{1}/metrics'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
