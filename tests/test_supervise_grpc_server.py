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
from logging import ERROR, Formatter, getLogger, INFO, StreamHandler
from tempfile import TemporaryDirectory

import grpc
from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf

from cockatrice import NAME
from cockatrice.protobuf.index_pb2 import ClearRequest, DeleteRequest, GetRequest, PutRequest
from cockatrice.protobuf.index_pb2_grpc import SuperviseStub
from cockatrice.supervise_core import SuperviseCore
from cockatrice.supervise_grpc_server import SuperviseGRPCServer
from tests import get_free_port


class TestSuperviseGRPCServer(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()
        self.example_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../example'))

        host = '0.0.0.0'
        port = get_free_port()
        peer_addrs = []
        snapshot_file = self.temp_dir.name + '/snapshot.zip'

        grpc_port = get_free_port()
        grpc_max_workers = 10

        federation_dir = self.temp_dir.name + '/federation'

        logger = getLogger(NAME)
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
        self.supervise_grpc_server = SuperviseGRPCServer(self.supervise_core, host=host, port=grpc_port,
                                                         max_workers=grpc_max_workers, logger=logger,
                                                         metrics_registry=metrics_registry)

        self.channel = grpc.insecure_channel('{0}:{1}'.format(host, grpc_port))

    def tearDown(self):
        self.channel.close()

        self.supervise_core.stop()
        self.supervise_grpc_server.stop()
        self.temp_dir.cleanup()

    def test_put(self):
        stub = SuperviseStub(self.channel)

        # put
        request = PutRequest()
        request.key = '/f1/c1/n1'
        request.value = pickle.dumps({'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'})
        request.sync = True

        response = stub.Put(request)

        self.assertEqual(True, response.status.success)

    def test_get(self):
        stub = SuperviseStub(self.channel)

        # put
        request = PutRequest()
        request.key = '/f1/c1/n1'
        request.value = pickle.dumps({'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'})
        request.sync = True

        response = stub.Put(request)

        self.assertEqual(True, response.status.success)

        # get
        request = GetRequest()
        request.key = '/f1/c1/n1'

        response = stub.Get(request)

        self.assertEqual(True, response.status.success)
        self.assertEqual({'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'}, pickle.loads(response.value))

        # get
        request = GetRequest()
        request.key = '/f1/c1/n1/grpc_addr'

        response = stub.Get(request)

        self.assertEqual(True, response.status.success)
        self.assertEqual('127.0.0.1:5050', pickle.loads(response.value))

    def test_delete(self):
        stub = SuperviseStub(self.channel)

        # put
        request = PutRequest()
        request.key = '/f1/c1/n1'
        request.value = pickle.dumps({'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'})
        request.sync = True

        response = stub.Put(request)

        self.assertTrue(response.status.success)

        # get
        request = GetRequest()
        request.key = '/f1/c1/n1'

        response = stub.Get(request)

        self.assertTrue(response.status.success)
        self.assertEqual({'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'}, pickle.loads(response.value))

        # get
        request = GetRequest()
        request.key = '/f1/c1/n1/grpc_addr'

        response = stub.Get(request)

        self.assertTrue(response.status.success)
        self.assertEqual('127.0.0.1:5050', pickle.loads(response.value))

        # delete
        request = DeleteRequest()
        request.key = '/f1/c1/n1/grpc_addr'
        request.sync = True

        response = stub.Delete(request)

        self.assertTrue(response.status.success)
        self.assertEqual('127.0.0.1:5050', pickle.loads(response.value))

        # get
        request = GetRequest()
        request.key = '/f1/c1/n1/grpc_addr'

        response = stub.Get(request)

        self.assertFalse(response.status.success)

        # delete
        request = DeleteRequest()
        request.key = '/'
        request.sync = True

        response = stub.Delete(request)

        self.assertTrue(response.status.success)

        # get
        request = GetRequest()
        request.key = '/'

        response = stub.Get(request)

        self.assertTrue(response.status.success)
        self.assertEqual({}, pickle.loads(response.value))

    def test_clear(self):
        stub = SuperviseStub(self.channel)

        # put
        request = PutRequest()
        request.key = '/f1/c1/n1'
        request.value = pickle.dumps({'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'})
        request.sync = True

        response = stub.Put(request)

        self.assertTrue(response.status.success)

        # get
        request = GetRequest()
        request.key = '/f1/c1/n1'

        response = stub.Get(request)

        self.assertTrue(response.status.success)
        self.assertEqual({'grpc_addr': '127.0.0.1:5050', 'http_addr': '127.0.0.1:8080'}, pickle.loads(response.value))

        # clear
        request = ClearRequest()
        request.sync = True

        response = stub.Clear(request)

        self.assertTrue(response.status.success)

        # get
        request = GetRequest()
        request.key = '/'

        response = stub.Get(request)

        self.assertTrue(response.status.success)
        self.assertEqual({}, pickle.loads(response.value))
