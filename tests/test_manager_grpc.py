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

import _pickle as pickle
import os
import unittest
from logging import ERROR, Formatter, getLogger, INFO, NOTSET, StreamHandler
from tempfile import TemporaryDirectory

import grpc
from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf

from cockatrice import NAME
from cockatrice.manager import Manager
from cockatrice.protobuf.management_pb2 import ClearRequest, DeleteRequest, GetRequest, PutRequest
from cockatrice.protobuf.management_pb2_grpc import ManagementStub
from tests import get_free_port


class TestManagementGRPCServicer(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.example_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../example'))

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

        self.manager = Manager(host=host, port=port, seed_addr=seed_addr, conf=conf, data_dir=data_dir,
                               grpc_port=grpc_port, grpc_max_workers=grpc_max_workers, http_port=http_port,
                               logger=logger, http_logger=http_logger, metrics_registry=metrics_registry)

        self.channel = grpc.insecure_channel('{0}:{1}'.format(host, grpc_port))
        self.stub = ManagementStub(self.channel)

    def tearDown(self):
        self.channel.close()

        self.manager.stop()

        self.temp_dir.cleanup()

    def test_put(self):
        # put
        request = PutRequest()
        request.key = '/f1/c1/n1'
        request.value = pickle.dumps({'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'})
        request.sync = True

        response = self.stub.Put(request)

        self.assertEqual(True, response.status.success)

    def test_get(self):
        # put
        request = PutRequest()
        request.key = '/f1/c1/n1'
        request.value = pickle.dumps({'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'})
        request.sync = True

        response = self.stub.Put(request)

        self.assertEqual(True, response.status.success)

        # get
        request = GetRequest()
        request.key = '/f1/c1/n1'

        response = self.stub.Get(request)

        self.assertEqual(True, response.status.success)
        self.assertEqual({'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'}, pickle.loads(response.value))

        # get
        request = GetRequest()
        request.key = '/f1/c1/n1/grpc_addr'

        response = self.stub.Get(request)

        self.assertEqual(True, response.status.success)
        self.assertEqual('127.0.0.1:5050', pickle.loads(response.value))

    def test_delete(self):
        # put
        request = PutRequest()
        request.key = '/f1/c1/n1'
        request.value = pickle.dumps({'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'})
        request.sync = True

        response = self.stub.Put(request)

        self.assertTrue(response.status.success)

        # get
        request = GetRequest()
        request.key = '/f1/c1/n1'

        response = self.stub.Get(request)

        self.assertTrue(response.status.success)
        self.assertEqual({'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'}, pickle.loads(response.value))

        # get
        request = GetRequest()
        request.key = '/f1/c1/n1/grpc_addr'

        response = self.stub.Get(request)

        self.assertTrue(response.status.success)
        self.assertEqual('127.0.0.1:5050', pickle.loads(response.value))

        # delete
        request = DeleteRequest()
        request.key = '/f1/c1/n1/grpc_addr'
        request.sync = True

        response = self.stub.Delete(request)

        self.assertTrue(response.status.success)
        self.assertEqual('127.0.0.1:5050', pickle.loads(response.value))

        # get
        request = GetRequest()
        request.key = '/f1/c1/n1/grpc_addr'

        response = self.stub.Get(request)

        self.assertFalse(response.status.success)

        # delete
        request = DeleteRequest()
        request.key = '/'
        request.sync = True

        response = self.stub.Delete(request)

        self.assertTrue(response.status.success)

        # get
        request = GetRequest()
        request.key = '/'

        response = self.stub.Get(request)

        self.assertTrue(response.status.success)
        self.assertEqual({}, pickle.loads(response.value))

    def test_clear(self):
        # put
        request = PutRequest()
        request.key = '/f1/c1/n1'
        request.value = pickle.dumps({'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'})
        request.sync = True

        response = self.stub.Put(request)

        self.assertTrue(response.status.success)

        # get
        request = GetRequest()
        request.key = '/f1/c1/n1'

        response = self.stub.Get(request)

        self.assertTrue(response.status.success)
        self.assertEqual({'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'}, pickle.loads(response.value))

        # clear
        request = ClearRequest()
        request.sync = True

        response = self.stub.Clear(request)

        self.assertTrue(response.status.success)

        # get
        request = GetRequest()
        request.key = '/'

        response = self.stub.Get(request)

        self.assertTrue(response.status.success)
        self.assertEqual({}, pickle.loads(response.value))
