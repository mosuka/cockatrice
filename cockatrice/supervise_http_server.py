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
import json
import time
from http import HTTPStatus
from logging import getLogger

import grpc
import mimeparse
import yaml
from flask import after_this_request, Flask, request
from prometheus_client.core import CollectorRegistry, Counter, Histogram

from cockatrice import NAME
from cockatrice.protobuf.index_pb2 import DeleteRequest, GetRequest, PutRequest
from cockatrice.protobuf.index_pb2_grpc import SuperviseStub
from cockatrice.util.http import HTTPServerThread, make_response

TRUE_STRINGS = ['true', 'yes', 'on', 't', 'y', '1']


class SuperviseHTTPServer:
    def __init__(self, grpc_port=5050, host='localhost', port=8080, logger=getLogger(), http_logger=getLogger(),
                 metrics_registry=CollectorRegistry()):
        self.__logger = logger
        self.__http_logger = http_logger
        self.__metrics_registry = metrics_registry

        self.__grpc_port = grpc_port
        self.__host = host
        self.__port = port

        self.app = Flask('supervise_http_server')
        self.app.add_url_rule('/config', endpoint='put_root', view_func=self.__put_root, methods=['PUT'])
        self.app.add_url_rule('/config', endpoint='get_root', view_func=self.__get_root, methods=['GET'])
        self.app.add_url_rule('/config', endpoint='delete_root', view_func=self.__delete_root, methods=['DELETE'])
        self.app.add_url_rule('/config/<path:key>', endpoint='put', view_func=self.__put, methods=['PUT'])
        self.app.add_url_rule('/config/<path:key>', endpoint='get', view_func=self.__get, methods=['GET'])
        self.app.add_url_rule('/config/<path:key>', endpoint='delete', view_func=self.__delete, methods=['DELETE'])

        # disable Flask default logger
        self.app.logger.disabled = True
        getLogger('werkzeug').disabled = True

        # metrics
        self.__metrics_http_requests_total = Counter(
            '{0}_supervise_http_requests_total'.format(NAME),
            'The number of requests.',
            [
                'method',
                'endpoint',
                'status_code'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_http_requests_bytes_total = Counter(
            '{0}_suopervise_http_requests_bytes_total'.format(NAME),
            'A summary of the invocation requests bytes.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_http_responses_bytes_total = Counter(
            '{0}_supervise_http_responses_bytes_total'.format(NAME),
            'A summary of the invocation responses bytes.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_http_requests_duration_seconds = Histogram(
            '{0}_supervise_http_requests_duration_seconds'.format(NAME),
            'The invocation duration in seconds.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )

        self.__grpc_channel = grpc.insecure_channel('{0}:{1}'.format(self.__host, self.__grpc_port))
        self.__supervise_stub = SuperviseStub(self.__grpc_channel)

        self.__http_server_thread = None
        try:
            # run server
            self.__http_server_thread = HTTPServerThread(self.__host, self.__port, self.app, logger=self.__logger)
            self.__http_server_thread.start()
            self.__logger.info('HTTP server has started')
        except Exception as ex:
            self.__logger.critical(ex)

    def stop(self):
        self.__http_server_thread.shutdown()

        self.__grpc_channel.close()

        self.__logger.info('HTTP server has stopped')

    def __record_http_log(self, req, resp):
        log_message = '{0} - {1} [{2}] "{3} {4} {5}" {6} {7} "{8}" "{9}"'.format(
            req.remote_addr,
            req.remote_user if req.remote_user is not None else '-',
            time.strftime('%d/%b/%Y %H:%M:%S +0000', time.gmtime()),
            req.method,
            req.path + ('?{0}'.format(req.query_string.decode('utf-8')) if len(req.query_string) > 0 else ''),
            req.environ.get('SERVER_PROTOCOL'),
            resp.status_code,
            resp.content_length,
            req.referrer if req.referrer is not None else '-',
            req.user_agent
        )
        self.__http_logger.info(log_message)

        return

    def __record_http_metrics(self, start_time, req, resp):
        self.__metrics_http_requests_total.labels(
            method=req.method,
            endpoint=req.path + ('?{0}'.format(req.query_string.decode('utf-8')) if len(req.query_string) > 0 else ''),
            status_code=resp.status_code.value
        ).inc()

        self.__metrics_http_requests_bytes_total.labels(
            method=req.method,
            endpoint=req.path + ('?{0}'.format(req.query_string.decode('utf-8')) if len(req.query_string) > 0 else '')
        ).inc(req.content_length if req.content_length is not None else 0)

        self.__metrics_http_responses_bytes_total.labels(
            method=req.method,
            endpoint=req.path + ('?{0}'.format(req.query_string.decode('utf-8')) if len(req.query_string) > 0 else '')
        ).inc(resp.content_length if resp.content_length is not None else 0)

        self.__metrics_http_requests_duration_seconds.labels(
            method=req.method,
            endpoint=req.path + ('?{0}'.format(req.query_string.decode('utf-8')) if len(req.query_string) > 0 else '')
        ).observe(time.time() - start_time)

        return

    def __put_root(self):
        return self.__put('/')

    def __put(self, key):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, response)
            self.__record_http_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            mime = mimeparse.parse_mime_type(request.headers.get('Content-Type'))
            charset = 'utf-8' if mime[2].get('charset') is None else mime[2].get('charset')
            if mime[1] == 'yaml':
                data_dict = yaml.safe_load(request.data.decode(charset))
            elif mime[1] == 'json':
                data_dict = json.loads(request.data.decode(charset))
            else:
                raise ValueError('unsupported format')

            if data_dict is None:
                raise ValueError('data is None')

            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            rpc_req = PutRequest()
            rpc_req.key = key if key.startswith('/') else '/' + key
            rpc_req.value = pickle.dumps(data_dict)
            rpc_req.sync = sync

            rpc_resp = self.__supervise_stub.Put(rpc_req)

            if sync:
                if rpc_resp.status.success:
                    status_code = HTTPStatus.CREATED
                else:
                    data['error'] = '{0}'.format(rpc_resp.status.message)
                    status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            else:
                status_code = HTTPStatus.ACCEPTED
        except (yaml.constructor.ConstructorError, json.decoder.JSONDecodeError, ValueError) as ex:
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

    def __get_root(self):
        return self.__get('/')

    def __get(self, key):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, response)
            self.__record_http_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            rpc_req = GetRequest()
            rpc_req.key = key if key.startswith('/') else '/' + key

            rpc_resp = self.__supervise_stub.Get(rpc_req)

            if rpc_resp.status.success:
                data['value'] = pickle.loads(rpc_resp.value)
                status_code = HTTPStatus.OK
            else:
                data['error'] = '{0}'.format(rpc_resp.status.message)
                if '{0} does not exist'.format(rpc_req.key) == rpc_resp.status.message:
                    status_code = HTTPStatus.NOT_FOUND
                else:
                    status_code = HTTPStatus.INTERNAL_SERVER_ERROR
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

    def __delete_root(self):
        return self.__get('/')

    def __delete(self, key):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, response)
            self.__record_http_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            rpc_req = DeleteRequest()
            rpc_req.key = key if key.startswith('/') else '/' + key
            rpc_req.sync = sync

            rpc_resp = self.__supervise_stub.Delete(rpc_req)

            if rpc_resp.status.success:
                status_code = HTTPStatus.OK
            else:
                data['error'] = '{0}'.format(rpc_resp.status.message)
                if '{0} does not exist'.format(rpc_req.key) == rpc_resp.status.message:
                    status_code = HTTPStatus.NOT_FOUND
                else:
                    status_code = HTTPStatus.INTERNAL_SERVER_ERROR
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
