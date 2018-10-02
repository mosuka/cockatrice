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

import json
import time

from http import HTTPStatus
from logging import getLogger

from flask import Flask, jsonify, request, after_this_request, Response
from prometheus_client.core import CollectorRegistry, Counter, Histogram, Gauge
from prometheus_client.exposition import CONTENT_TYPE_LATEST, generate_latest

from basilisk import APP_NAME

TRUE_STRS = ["true", "yes", "on", "t", "y", "1"]


class KVSServer:
    def __init__(self, name, port,
                 kvs, logger=getLogger(APP_NAME), http_logger=getLogger(APP_NAME + "_http"),
                 metrics_registry=CollectorRegistry()):
        self.logger = logger
        self.http_logger = http_logger
        self.metrics_registry = metrics_registry

        self.port = port
        self.kvs = kvs

        self.app = Flask(name)
        self.app.add_url_rule("/rest/kvs/<key>", "get", self.get, methods=["GET"])
        self.app.add_url_rule("/rest/kvs/<key>", "put", self.put, methods=["PUT"])
        self.app.add_url_rule("/rest/kvs/<key>", "delete", self.delete, methods=["DELETE"])
        self.app.add_url_rule("/metrics", "metrics", self.metrics, methods=["GET"])

        # disable Flask default logger
        self.app.logger.disabled = True
        getLogger("werkzeug").disabled = True

        # metrics
        self.metrics_http_requests_total = Counter(
            "http_requests_total",
            "The number of requests.",
            [
                "method",
                "endpoint",
                "status_code"
            ],
            registry=self.metrics_registry
        )
        self.metrics_http_requests_bytes_total = Counter(
            "http_requests_bytes_total",
            "A summary of the invocation requests bytes.",
            [
                "method",
                "endpoint"
            ],
            registry=self.metrics_registry
        )
        self.metrics_http_responses_bytes_total = Counter(
            "http_responses_bytes_total",
            "A summary of the invocation responses bytes.",
            [
                "method",
                "endpoint"
            ],
            registry=self.metrics_registry
        )
        self.metrics_http_requests_duration_seconds = Histogram(
            "http_requests_duration_seconds",
            "The invocation duration in seconds.",
            [
                "method",
                "endpoint"
            ],
            registry=self.metrics_registry
        )
        self.metrics_kvs_records_count = Gauge(
            "kvs_records_count",
            "The number of kvs records.",
            registry=self.metrics_registry
        )

    def start(self):
        try:
            self.app.run(host="0.0.0.0", port=self.port)
        except OSError as ex:
            self.logger.critical(ex)
        except Exception as ex:
            self.logger.critical(ex)

    def record_http_log(self, req, resp):
        log_message = "{0} - {1} [{2}] \"{3} {4} {5}\" {6} {7} \"{8}\" \"{9}\"".format(
            req.remote_addr,
            req.remote_user if req.remote_user is not None else "-",
            time.strftime("%d/%b/%Y %H:%M:%S +0000", time.gmtime()),
            req.method,
            req.path + ("?{0}".format(req.query_string.decode("utf-8")) if len(req.query_string) > 0 else ""),
            req.environ.get("SERVER_PROTOCOL"),
            resp.status_code,
            resp.content_length,
            req.referrer if req.referrer is not None else "-",
            req.user_agent
        )
        self.http_logger.info(log_message)

        return

    def record_metrics(self, start_time, req, resp):
        self.metrics_http_requests_total.labels(
            method=req.method,
            endpoint=req.path + ("?{0}".format(req.query_string.decode("utf-8")) if len(req.query_string) > 0 else ""),
            status_code=resp.status_code.value
        ).inc()

        self.metrics_http_requests_bytes_total.labels(
            method=req.method,
            endpoint=req.path + ("?{0}".format(req.query_string.decode("utf-8")) if len(req.query_string) > 0 else "")
        ).inc(req.content_length if req.content_length is not None else 0)

        self.metrics_http_responses_bytes_total.labels(
            method=req.method,
            endpoint=req.path + ("?{0}".format(req.query_string.decode("utf-8")) if len(req.query_string) > 0 else "")
        ).inc(resp.content_length if resp.content_length is not None else 0)

        self.metrics_http_requests_duration_seconds.labels(
            method=req.method,
            endpoint=req.path + ("?{0}".format(req.query_string.decode("utf-8")) if len(req.query_string) > 0 else "")
        ).observe(time.time() - start_time)

        self.metrics_kvs_records_count.set(self.kvs.len())

        return

    def post_process(self, start_time, req, resp):
        self.record_http_log(req, resp)
        self.record_metrics(start_time, req, resp)

        return resp

    def get(self, key):
        start_time = time.time()

        data = {}
        doc = {}
        status_code = None

        @after_this_request
        def to_do_after_this_request(response):
            return self.post_process(start_time, request, response)

        try:
            value = self.kvs.get(key)

            if value is None:
                raise KeyError("{0} does not exist".format(key))

            doc["id"] = key
            doc["fields"] = value
            status_code = HTTPStatus.OK
        except KeyError as ex:
            data["error"] = "{0}".format(ex.args[0])
            status_code = HTTPStatus.NOT_FOUND
            self.logger.error(ex)
        except Exception as ex:
            data["error"] = "{0}".format(ex.args[0])
            status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            self.logger.error(ex)
        finally:
            data["time"] = time.time() - start_time
            data["status"] = {"code": status_code.value, "phrase": status_code.phrase,
                              "description": status_code.description}
            data["doc"] = doc

        resp = jsonify(data)
        resp.status_code = status_code

        return resp

    def put(self, key):
        start_time = time.time()

        data = {}
        status_code = None

        @after_this_request
        def to_do_after_this_request(response):
            return self.post_process(start_time, request, response)

        try:
            sync = False
            if request.args.get("sync", default="", type=str).lower() in TRUE_STRS:
                sync = True

            value = json.loads(request.data)
            self.kvs.set(key, value, sync)
            status_code = HTTPStatus.CREATED
        except json.decoder.JSONDecodeError as ex:
            data["error"] = "{0}".format(ex.args[0])
            status_code = HTTPStatus.BAD_REQUEST
            self.logger.error(ex)
        except Exception as ex:
            data["error"] = "{0}".format(ex.args[0])
            status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            self.logger.error(ex)
        finally:
            data["time"] = time.time() - start_time
            data["status"] = {"code": status_code.value, "phrase": status_code.phrase,
                              "description": status_code.description}

        resp = jsonify(data)
        resp.status_code = status_code

        return resp

    def delete(self, key):
        start_time = time.time()

        data = {}
        status_code = None

        @after_this_request
        def to_do_after_this_request(response):
            return self.post_process(start_time, request, response)

        try:
            sync = False
            if request.args.get("sync", default="", type=str).lower() in TRUE_STRS:
                sync = True

            self.kvs.delete(key, sync)
            status_code = HTTPStatus.ACCEPTED
        except KeyError as ex:
            data["error"] = "{0}".format(ex.args[0])
            status_code = HTTPStatus.NOT_FOUND
            self.logger.error(ex)
        except Exception as ex:
            data["error"] = "{0}".format(ex.args[0])
            status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            self.logger.error(ex)
        finally:
            data["time"] = time.time() - start_time
            data["status"] = {"code": status_code.value, "phrase": status_code.phrase,
                              "description": status_code.description}

        resp = jsonify(data)
        resp.status_code = status_code

        return resp

    def metrics(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            return self.post_process(start_time, request, response)

        resp = Response()
        status_code = None

        try:
            resp.data = generate_latest(self.metrics_registry)
            status_code = HTTPStatus.OK
        except Exception as ex:
            resp.data = "{0}\n{1}".format(status_code.phrase, status_code.description)
            status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            self.logger.error(ex)

        resp.status_code = status_code
        resp.content_type = CONTENT_TYPE_LATEST

        return resp
