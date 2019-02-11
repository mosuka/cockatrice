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

import requests
from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf

from cockatrice import NAME
from cockatrice.manager import Manager
from tests import get_free_port


class TestManagementHTTPServicer(unittest.TestCase):
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

        self.supervise_core = Manager(host=host, port=port, seed_addr=seed_addr, conf=conf, data_dir=data_dir,
                                      grpc_port=grpc_port, grpc_max_workers=grpc_max_workers, http_port=http_port,
                                      logger=logger, http_logger=http_logger, metrics_registry=metrics_registry)

        self.host = host
        self.port = http_port

    def tearDown(self):
        self.supervise_core.stop()
        self.temp_dir.cleanup()

    def test_put(self):
        data = '{"a": {"b": {"c": 1, "d": 2}}}'

        # put data
        response = requests.put('http://{0}:{1}/config?sync=True'.format(self.host, self.port),
                                headers={'Content-Type': 'application/json'}, data=data.encode('utf-8'))

        self.assertEqual(HTTPStatus.CREATED, response.status_code)

    def test_get(self):
        data = '{"a": {"b": {"c": 1, "d": 2}}}'

        # put data
        response = requests.put('http://{0}:{1}/config?sync=True'.format(self.host, self.port),
                                headers={'Content-Type': 'application/json'}, data=data.encode('utf-8'))
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # get data
        response = requests.get('http://{0}:{1}/config/a/b/c'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        resp_data = json.loads(response.text)
        self.assertEqual(1, resp_data['value'])

    def test_delete(self):
        data = '{"a": {"b": {"c": 1, "d": 2}}}'

        # put data
        response = requests.put('http://{0}:{1}/config?sync=True'.format(self.host, self.port),
                                headers={'Content-Type': 'application/json'}, data=data.encode('utf-8'))
        self.assertEqual(HTTPStatus.CREATED, response.status_code)

        # get data
        response = requests.get('http://{0}:{1}/config/a/b/c'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
        resp_data = json.loads(response.text)
        self.assertEqual(1, resp_data['value'])

        # delete data
        response = requests.delete('http://{0}:{1}/config/a/b/c?sync=True'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get data
        response = requests.get('http://{0}:{1}/config/a/b/c'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.NOT_FOUND, response.status_code)

        # delete data
        response = requests.delete('http://{0}:{1}/config?sync=True'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)

        # get data
        response = requests.get('http://{0}:{1}/config'.format(self.host, self.port))
        self.assertEqual(HTTPStatus.OK, response.status_code)
