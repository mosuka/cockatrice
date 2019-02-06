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
from http import HTTPStatus
from logging import ERROR, Formatter, getLogger, INFO, NOTSET, StreamHandler
from tempfile import TemporaryDirectory

from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf

from cockatrice import NAME
from cockatrice.supervise_core import SuperviseCore
from cockatrice.supervise_grpc_server import SuperviseGRPCServer
from cockatrice.supervise_http_server import SuperviseHTTPServer
from tests import get_free_port, get_test_client


class TestSuperviseHTTPServer(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.example_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../example'))

        host = '0.0.0.0'
        port = get_free_port()
        peer_addrs = []
        dump_file = self.temp_dir.name + '/supervise.zip'

        grpc_port = get_free_port()
        grpc_max_workers = 10

        http_port = get_free_port()

        federation_dir = self.temp_dir.name + '/supervise'

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

        self.supervise_core = SuperviseCore(host=host, port=port, peer_addrs=peer_addrs, conf=conf,
                                            data_dir=federation_dir, logger=logger,
                                            metrics_registry=metrics_registry)
        self.supervise_grpc_server = SuperviseGRPCServer(self.supervise_core, host=host, port=grpc_port,
                                                         max_workers=grpc_max_workers, logger=logger,
                                                         metrics_registry=metrics_registry)
        self.supervise_http_server = SuperviseHTTPServer(grpc_port, host, http_port, logger=logger,
                                                         http_logger=http_logger, metrics_registry=metrics_registry)

        self.test_client = get_test_client(self.supervise_http_server.app)

    def tearDown(self):
        self.supervise_core.stop()
        self.supervise_http_server.stop()
        self.temp_dir.cleanup()

    def test_put(self):
        data = '{"a": {"b": {"c": 1, "d": 2}}}'

        # put data
        response = self.test_client.put('/config', data=data, query_string='sync=True',
                                        headers={'Content-Type': 'application/yaml'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

    def test_get(self):
        data = '{"a": {"b": {"c": 1, "d": 2}}}'

        # put data
        response = self.test_client.put('/config', data=data, query_string='sync=True',
                                        headers={'Content-Type': 'application/json'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # get data
        response = self.test_client.get('/config/a/b/c')
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.data)
        self.assertEqual(1, data['value'])

    def test_delete(self):
        data = '{"a": {"b": {"c": 1, "d": 2}}}'

        # put data
        response = self.test_client.put('/config', data=data, query_string='sync=True',
                                        headers={'Content-Type': 'application/json'})
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # get data
        response = self.test_client.get('/config/a/b/c')
        self.assertEqual(HTTPStatus.OK, response.status_code)
        data = json.loads(response.data)
        self.assertEqual(1, data['value'])

        # delete data
        response = self.test_client.delete('/config/a/b/c', query_string='sync=True')
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get data
        response = self.test_client.get('/config/a/b/c')
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

        # delete data
        response = self.test_client.delete('/config', query_string='sync=True')
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get data
        response = self.test_client.get('/config')
        self.assertEqual(HTTPStatus.OK, response.status_code)
