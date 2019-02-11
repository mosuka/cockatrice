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
from logging import ERROR, Formatter, getLogger, INFO, NOTSET, StreamHandler
from tempfile import TemporaryDirectory

from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf

from cockatrice import NAME
from cockatrice.manager import Manager
from tests import get_free_port


class TestManager(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()

        host = '0.0.0.0'
        port = get_free_port()
        seed_addr = None
        conf = SyncObjConf(
            fullDumpFile=self.temp_dir.name + '/supervise.zip',
            logCompactionMinTime=300,
            dynamicMembershipChange=True
        )
        data_dir = self.temp_dir.name + '/supervise'
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

        self.supervise_core = Manager(host=host, port=port, seed_addr=seed_addr, conf=conf, data_dir=data_dir,
                                      grpc_port=grpc_port, grpc_max_workers=grpc_max_workers, http_port=http_port,
                                      logger=logger, http_logger=http_logger, metrics_registry=metrics_registry)

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
