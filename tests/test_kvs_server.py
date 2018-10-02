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

import json

from tempfile import TemporaryDirectory
from logging import getLogger, StreamHandler, Formatter, DEBUG, INFO
from http import HTTPStatus

from prometheus_client.core import CollectorRegistry

from basilisk import APP_NAME
from basilisk.kvs import KeyValueStore
from basilisk.kvs_server import KVSServer


class TestKVSServer(unittest.TestCase):
    def setUp(self):
        self.temp_dir = TemporaryDirectory()

        http_port = 0
        bind_addr = "0.0.0.0:0"
        peer_addrs = []
        dump_file = self.temp_dir.name + "/data.dump"

        logger = getLogger(APP_NAME)
        log_handler = StreamHandler()
        logger.setLevel(DEBUG)
        log_handler.setLevel(INFO)
        log_format = Formatter('%(asctime)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s')
        log_handler.setFormatter(log_format)
        logger.addHandler(log_handler)

        http_logger = getLogger(APP_NAME + "_http")
        http_log_handler = StreamHandler()
        http_logger.setLevel(INFO)
        http_log_handler.setLevel(INFO)
        http_log_format = Formatter('%(message)s')
        http_log_handler.setFormatter(http_log_format)
        http_logger.addHandler(http_log_handler)

        metrics_registry = CollectorRegistry()

        self.kvs = KeyValueStore(bind_addr, peer_addrs, dump_file)
        self.server = KVSServer(APP_NAME, http_port, self.kvs, logger, http_logger, metrics_registry)

        self.client = self.server.app.test_client()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_metrics(self):
        response = self.client.get("/metrics")

        expected_status_code = HTTPStatus.OK
        actual_status_code = response.status_code

        self.assertEqual(expected_status_code, actual_status_code)

    def test_put(self):
        fields = {"name": "example"}

        response = self.client.put("/rest/kvs/1", data=json.dumps(fields))

        expected_status_code = HTTPStatus.CREATED
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        data = json.loads(response.data)

        self.assertTrue("status" in data)

        expected_status_code = HTTPStatus.CREATED
        actual_status_code = data["status"]["code"]
        self.assertEqual(expected_status_code, actual_status_code)

    def test_get(self):
        fields = {"name": "example"}

        self.client.put("/rest/kvs/2", data=json.dumps(fields), query_string="sync=True")
        response = self.client.get("/rest/kvs/2")

        expected_status_code = HTTPStatus.OK
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        data = json.loads(response.data)

        expected_status_code = HTTPStatus.OK
        actual_status_code = data["status"]["code"]
        self.assertEqual(expected_status_code, actual_status_code)

        expected_doc_id = "2"
        actual_doc_id = data["doc"]["id"]
        self.assertEqual(expected_doc_id, actual_doc_id)

        self.assertTrue("fields" in data["doc"])

    def test_delete(self):
        fields = {"name": "example"}

        self.client.put("/rest/kvs/3", data=json.dumps(fields), query_string="sync=True")
        response = self.client.delete("/rest/kvs/3", query_string="sync=True")

        expected_status_code = HTTPStatus.ACCEPTED
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)

        response = self.client.get("/rest/kvs/3")

        expected_status_code = HTTPStatus.NOT_FOUND
        actual_status_code = response.status_code
        self.assertEqual(expected_status_code, actual_status_code)
