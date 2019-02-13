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
import time
from http import HTTPStatus
from json import JSONDecodeError
from logging import getLogger

import mimeparse
import yaml
from flask import after_this_request, Flask, request
from prometheus_client.core import CollectorRegistry, Counter, Histogram
from yaml.constructor import ConstructorError

from cockatrice import NAME
from cockatrice.util.http import make_response, record_log, TRUE_STRINGS


class ManagementHTTPServicer:
    def __init__(self, manager, logger=getLogger(), http_logger=getLogger(),
                 metrics_registry=CollectorRegistry()):
        self.__manager = manager
        self.__logger = logger
        self.__http_logger = http_logger
        self.__metrics_registry = metrics_registry

        # metrics
        self.__metrics_requests_total = Counter(
            '{0}_manager_http_requests_total'.format(NAME),
            'The number of requests.',
            [
                'method',
                'endpoint',
                'status_code'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_requests_duration_seconds = Histogram(
            '{0}_manager_http_requests_duration_seconds'.format(NAME),
            'The invocation duration in seconds.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_requests_bytes_total = Counter(
            '{0}_manager_http_requests_bytes_total'.format(NAME),
            'A summary of the invocation requests bytes.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_responses_bytes_total = Counter(
            '{0}_manager_http_responses_bytes_total'.format(NAME),
            'A summary of the invocation responses bytes.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )

        self.app = Flask('supervise_http_server')
        self.app.add_url_rule('/data', endpoint='put', view_func=self.__put, methods=['PUT'], strict_slashes=False)
        self.app.add_url_rule('/data', endpoint='get', view_func=self.__get, methods=['GET'], strict_slashes=False)
        self.app.add_url_rule('/data', endpoint='delete', view_func=self.__delete, methods=['DELETE'],
                              strict_slashes=False)
        self.app.add_url_rule('/data/<path:key>', endpoint='put', view_func=self.__put, methods=['PUT'])
        self.app.add_url_rule('/data/<path:key>', endpoint='get', view_func=self.__get, methods=['GET'])
        self.app.add_url_rule('/data/<path:key>', endpoint='delete', view_func=self.__delete, methods=['DELETE'])

        # disable Flask default logger
        self.app.logger.disabled = True
        getLogger('werkzeug').disabled = True

    def __record_metrics(self, start_time, req, resp):
        self.__metrics_requests_total.labels(
            method=req.method,
            endpoint=req.path + ('?{0}'.format(req.query_string.decode('utf-8')) if len(req.query_string) > 0 else ''),
            status_code=resp.status_code.value
        ).inc()

        self.__metrics_requests_bytes_total.labels(
            method=req.method,
            endpoint=req.path + ('?{0}'.format(req.query_string.decode('utf-8')) if len(req.query_string) > 0 else '')
        ).inc(req.content_length if req.content_length is not None else 0)

        self.__metrics_responses_bytes_total.labels(
            method=req.method,
            endpoint=req.path + ('?{0}'.format(req.query_string.decode('utf-8')) if len(req.query_string) > 0 else '')
        ).inc(resp.content_length if resp.content_length is not None else 0)

        self.__metrics_requests_duration_seconds.labels(
            method=req.method,
            endpoint=req.path + ('?{0}'.format(req.query_string.decode('utf-8')) if len(req.query_string) > 0 else '')
        ).observe(time.time() - start_time)

        return

    def __put(self, key=''):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            mime = mimeparse.parse_mime_type(request.headers.get('Content-Type'))
            charset = 'utf-8' if mime[2].get('charset') is None else mime[2].get('charset')
            if mime[1] == 'yaml':
                value = yaml.safe_load(request.data.decode(charset))
            elif mime[1] == 'json':
                value = json.loads(request.data.decode(charset))
            else:
                # handle as a string
                value = request.data.decode(charset)

            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            self.__manager.put(key if key.startswith('/') else '/' + key, value, sync=sync)

            if sync:
                status_code = HTTPStatus.CREATED
            else:
                status_code = HTTPStatus.ACCEPTED
        except (ConstructorError, JSONDecodeError, ValueError) as ex:
            data['error'] = '{0}'.format(ex.args[0])
            status_code = HTTPStatus.BAD_REQUEST
            self.__logger.error(ex)
        except Exception as ex:
            data['error'] = '{0}'.format(ex.args[0])
            status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            self.__logger.error(ex)
        finally:
            data['time'] = time.time() - start_time
            data['status'] = {'code': status_code.value, 'phrase': status_code.phrase,
                              'description': status_code.description}

        output = request.args.get('output', default='json', type=str).lower()

        # make response
        resp = make_response(data, output)
        resp.status_code = status_code

        return resp

    def __get(self, key=''):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            value = self.__manager.get(key if key.startswith('/') else '/' + key)

            if value is None:
                status_code = HTTPStatus.NOT_FOUND
            else:
                data['value'] = value
                status_code = HTTPStatus.OK
        except Exception as ex:
            data['error'] = '{0}'.format(ex.args[0])
            status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            self.__logger.error(ex)
        finally:
            data['time'] = time.time() - start_time
            data['status'] = {'code': status_code.value, 'phrase': status_code.phrase,
                              'description': status_code.description}

        output = request.args.get('output', default='json', type=str).lower()

        # make response
        resp = make_response(data, output)
        resp.status_code = status_code

        return resp

    def __delete(self, key=''):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            value = self.__manager.delete(key if key.startswith('/') else '/' + key, sync=sync)

            if value is None:
                status_code = HTTPStatus.NOT_FOUND
            else:
                status_code = HTTPStatus.OK
        except Exception as ex:
            data['error'] = '{0}'.format(ex.args[0])
            status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            self.__logger.error(ex)
        finally:
            data['time'] = time.time() - start_time
            data['status'] = {'code': status_code.value, 'phrase': status_code.phrase,
                              'description': status_code.description}

        output = request.args.get('output', default='json', type=str).lower()

        # make response
        resp = make_response(data, output)
        resp.status_code = status_code

        return resp
