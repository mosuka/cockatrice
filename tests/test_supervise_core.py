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

import unittest
from logging import ERROR, Formatter, getLogger, INFO, StreamHandler
from tempfile import TemporaryDirectory

from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf

import cockatrice
from cockatrice.supervise_core import SuperviseCore
from tests import get_free_port


class TestSuperviseCore(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()

        host = '0.0.0.0'
        port = get_free_port()
        peer_addrs = []
        snapshot_file = self.temp_dir.name + '/federation.zip'

        federation_dir = self.temp_dir.name + '/federation'

        logger = getLogger(cockatrice.NAME)
        log_handler = StreamHandler()
        logger.setLevel(ERROR)
        log_handler.setLevel(INFO)
        log_format = Formatter('%(asctime)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s')
        log_handler.setFormatter(log_format)
        logger.addHandler(log_handler)

        metrics_registry = CollectorRegistry()

        conf = SyncObjConf(
            fullDumpFile=snapshot_file,
            logCompactionMinTime=300,
            dynamicMembershipChange=True
        )

        self.supervise_core = SuperviseCore(host=host, port=port, peer_addrs=peer_addrs, conf=conf,
                                            data_dir=federation_dir, logger=logger,
                                            metrics_registry=metrics_registry)

    def tearDown(self):
        self.supervise_core.stop()
        self.temp_dir.cleanup()

    def test_is_healthy(self):
        self.assertTrue(self.supervise_core.is_healthy())

    def test_is_alive(self):
        self.assertTrue(self.supervise_core.is_alive())

    def test_is_ready(self):
        self.assertTrue(self.supervise_core.is_ready())

    def test_put(self):
        self.supervise_core.put('/f1/c1/n1', {'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'}, sync=True)

        self.supervise_core.put('/', {'a': 1, 'b': 2}, sync=True)

    def test_get(self):
        self.supervise_core.put('f1/c1/n1', {'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'}, sync=True)

        data = self.supervise_core.get('/f1/c1/n1')
        self.assertEqual({'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'}, data)

        data = self.supervise_core.get('/f1/c1/n1/grpc_addr')
        self.assertEqual('127.0.0.1:5050', data)

        self.supervise_core.clear(sync=True)

        self.supervise_core.put('/', {'a': 1, 'b': 2}, sync=True)

        data = self.supervise_core.get('/')
        self.assertEqual({'a': 1, 'b': 2}, data)

    def test_delete(self):
        self.supervise_core.put('f1/c1/n1', {'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'}, sync=True)

        data = self.supervise_core.get('/f1/c1/n1')
        self.assertEqual({'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'}, data)

        data = self.supervise_core.get('/f1/c1/n1/grpc_addr')
        self.assertEqual('127.0.0.1:5050', data)

        data = self.supervise_core.delete('/f1/c1/n1/grpc_addr', sync=True)
        self.assertEqual('127.0.0.1:5050', data)

        data = self.supervise_core.get('/f1/c1/n1/grpc_addr')
        self.assertEqual(None, data)

        data = self.supervise_core.get('/f1/c1/n1')
        self.assertEqual({'http_addr': '127.0.0.1:8080'}, data)

        data = self.supervise_core.get('/f1/c1')
        self.assertEqual({'n1': {'http_addr': '127.0.0.1:8080'}}, data)

        data = self.supervise_core.delete('/', sync=True)
        self.assertEqual(None, data)

    def test_clear(self):
        self.supervise_core.put('f1/c1/n1', {'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'}, sync=True)

        data = self.supervise_core.get('/f1/c1/n1')
        self.assertEqual({'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'}, data)

        self.supervise_core.clear(sync=True)

        data = self.supervise_core.get('/')
        self.assertEqual({}, data)
