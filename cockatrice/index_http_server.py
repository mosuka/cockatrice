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
from threading import Thread

import grpc
import mimeparse
import yaml
from flask import after_this_request, Flask, request, Response
from prometheus_client.core import CollectorRegistry, Counter, Histogram
from prometheus_client.exposition import CONTENT_TYPE_LATEST, generate_latest
from werkzeug.serving import make_server
from yaml.constructor import ConstructorError

from cockatrice import NAME, VERSION
from cockatrice.protobuf.index_pb2 import CreateIndexRequest, CreateSnapshotRequest, DeleteDocumentRequest, \
    DeleteDocumentsRequest, DeleteIndexRequest, DeleteNodeRequest, GetDocumentRequest, GetIndexRequest, \
    GetSnapshotRequest, GetStatusRequest, IsAliveRequest, IsHealthyRequest, IsReadyRequest, OptimizeIndexRequest, \
    PutDocumentRequest, PutDocumentsRequest, PutNodeRequest, SearchDocumentsRequest, SnapshotExistsRequest
from cockatrice.protobuf.index_pb2_grpc import IndexStub

TRUE_STRINGS = ['true', 'yes', 'on', 't', 'y', '1']


class IndexHTTPServerThread(Thread):
    def __init__(self, host, port, app, logger=getLogger()):
        self.__logger = logger

        Thread.__init__(self)
        self.server = make_server(host, port, app)
        self.context = app.app_context()
        self.context.push()

    def run(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()


class IndexHTTPServer:
    def __init__(self, grpc_port=5050, host='localhost', port=8080, logger=getLogger(), http_logger=getLogger(),
                 metrics_registry=CollectorRegistry()):
        self.__logger = logger
        self.__http_logger = http_logger
        self.__metrics_registry = metrics_registry

        self.__grpc_port = grpc_port
        self.__host = host
        self.__port = port

        self.__app = Flask('index_http_server')
        self.__app.add_url_rule('/', endpoint='root', view_func=self.__root, methods=['GET'])
        self.__app.add_url_rule('/indices/<index_name>', endpoint='get_index', view_func=self.__get_index,
                                methods=['GET'])
        self.__app.add_url_rule('/indices/<index_name>', endpoint='create_index', view_func=self.__create_index,
                                methods=['PUT'])
        self.__app.add_url_rule('/indices/<index_name>', endpoint='delete_index', view_func=self.__delete_index,
                                methods=['DELETE'])
        self.__app.add_url_rule('/indices/<index_name>/documents/<doc_id>', endpoint='get_document',
                                view_func=self.__get_document, methods=['GET'])
        self.__app.add_url_rule('/indices/<index_name>/documents/<doc_id>', endpoint='put_document',
                                view_func=self.__put_document, methods=['PUT'])
        self.__app.add_url_rule('/indices/<index_name>/documents/<doc_id>', endpoint='delete_document',
                                view_func=self.__delete_document, methods=['DELETE'])
        self.__app.add_url_rule('/indices/<index_name>/documents', endpoint='put_documents',
                                view_func=self.__put_documents, methods=['PUT'])
        self.__app.add_url_rule('/indices/<index_name>/documents', endpoint='delete_documents',
                                view_func=self.__delete_documents, methods=['DELETE'])
        self.__app.add_url_rule('/indices/<index_name>/search', endpoint='search_documents',
                                view_func=self.__search_documents, methods=['GET', 'POST'])
        self.__app.add_url_rule('/indices/<index_name>/optimize', endpoint='optimize_index',
                                view_func=self.__optimize_index, methods=['GET'])
        self.__app.add_url_rule('/nodes/<node_name>', endpoint='put_node', view_func=self.__put_node, methods=['PUT'])
        self.__app.add_url_rule('/nodes/<node_name>', endpoint='delete_node', view_func=self.__delete_node,
                                methods=['DELETE'])
        self.__app.add_url_rule('/snapshot', endpoint='get_snapshot', view_func=self.__get_snapshot, methods=['GET'])
        self.__app.add_url_rule('/snapshot', endpoint='put_snapshot', view_func=self.__put_snapshot, methods=['PUT'])
        self.__app.add_url_rule('/metrics', endpoint='metrics', view_func=self.__metrics, methods=['GET'])
        self.__app.add_url_rule('/healthiness', endpoint='healthiness', view_func=self.__healthiness, methods=['GET'])
        self.__app.add_url_rule('/liveness', endpoint='liveness', view_func=self.__liveness, methods=['GET'])
        self.__app.add_url_rule('/readiness', endpoint='readiness', view_func=self.__readiness, methods=['GET'])
        self.__app.add_url_rule('/status', endpoint='status', view_func=self.__get_status, methods=['GET'])

        # disable Flask default logger
        self.__app.logger.disabled = True
        getLogger('werkzeug').disabled = True

        # metrics
        self.__metrics_http_requests_total = Counter(
            '{0}_index_http_requests_total'.format(NAME),
            'The number of requests.',
            [
                'method',
                'endpoint',
                'status_code'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_http_requests_bytes_total = Counter(
            '{0}_index_http_requests_bytes_total'.format(NAME),
            'A summary of the invocation requests bytes.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_http_responses_bytes_total = Counter(
            '{0}_index_http_responses_bytes_total'.format(NAME),
            'A summary of the invocation responses bytes.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_http_requests_duration_seconds = Histogram(
            '{0}_index_http_requests_duration_seconds'.format(NAME),
            'The invocation duration in seconds.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )

        self.__grpc_channel = grpc.insecure_channel('{0}:{1}'.format(self.__host, self.__grpc_port))
        self.__index_stub = IndexStub(self.__grpc_channel)

        self.__http_server_thread = None
        try:
            # run server
            self.__http_server_thread = IndexHTTPServerThread(self.__host, self.__port, self.__app,
                                                              logger=self.__logger)
            self.__http_server_thread.start()
            self.__logger.info('HTTP server started')
        except Exception as ex:
            self.__logger.critical(ex)

    def stop(self):
        self.__http_server_thread.shutdown()

        self.__grpc_channel.close()

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

    def __root(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, response)
            self.__record_http_metrics(start_time, request, response)
            return response

        resp = Response()

        try:
            resp.status_code = HTTPStatus.OK
            resp.content_type = 'text/plain; charset="UTF-8"'
            resp.data = NAME + ' ' + VERSION + ' is running.\n'
        except Exception as ex:
            resp.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            resp.content_type = 'text/plain; charset="UTF-8"'
            resp.data = '{0}\n{1}'.format(resp.status_code.phrase, resp.status_code.description)
            self.__logger.error(ex)

        return resp

    def __create_index(self, index_name):
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
                schema_dict = yaml.safe_load(request.data.decode(charset))
            elif mime[1] in ['application/json']:
                schema_dict = json.loads(request.data.decode(charset))
            else:
                raise ValueError('unsupported format')

            if schema_dict is None:
                raise ValueError('schema is None')

            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            rpc_req = CreateIndexRequest()
            rpc_req.index_name = index_name
            rpc_req.schema = pickle.dumps(schema_dict)
            rpc_req.sync = sync

            rpc_resp = self.__index_stub.CreateIndex(rpc_req)

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

    def __get_index(self, index_name):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, response)
            self.__record_http_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            rpc_req = GetIndexRequest()
            rpc_req.index_name = index_name

            rpc_resp = self.__index_stub.GetIndex(rpc_req)

            if rpc_resp.status.success:
                data['index_stats'] = {
                    'name': rpc_resp.index_stats.name,
                    'doc_count': rpc_resp.index_stats.doc_count,
                    'doc_count_all': rpc_resp.index_stats.doc_count_all,
                    'last_modified': rpc_resp.index_stats.last_modified,
                    'latest_generation': rpc_resp.index_stats.latest_generation,
                    'version': rpc_resp.index_stats.version,
                    'storage': {
                        'folder': rpc_resp.index_stats.storage.folder,
                        'supports_mmap': rpc_resp.index_stats.storage.supports_mmap,
                        'readonly': rpc_resp.index_stats.storage.readonly,
                        'files': [file for file in rpc_resp.index_stats.storage.files]
                    }
                }
                status_code = HTTPStatus.OK
            else:
                data['error'] = '{0}'.format(rpc_resp.status.message)
                if '{0} does not exist'.format(rpc_req.index_name) == rpc_resp.status.message:
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

    def __delete_index(self, index_name):
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

            rpc_req = DeleteIndexRequest()
            rpc_req.index_name = index_name
            rpc_req.sync = sync

            rpc_resp = self.__index_stub.DeleteIndex(rpc_req)

            if sync:
                if rpc_resp.status.success:
                    status_code = HTTPStatus.OK
                else:
                    data['error'] = '{0}'.format(rpc_resp.status.message)
                    status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            else:
                status_code = HTTPStatus.ACCEPTED
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

    def __optimize_index(self, index_name):
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

            rpc_req = OptimizeIndexRequest()
            rpc_req.index_name = index_name
            rpc_req.sync = sync

            rpc_resp = self.__index_stub.OptimizeIndex(rpc_req)

            if sync:
                if rpc_resp.status.success:
                    status_code = HTTPStatus.OK
                else:
                    data['error'] = '{0}'.format(rpc_resp.status.message)
                    status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            else:
                status_code = HTTPStatus.ACCEPTED
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

    def __put_document(self, index_name, doc_id):
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
                fields_dict = yaml.safe_load(request.data.decode(charset))
            elif mime[1] == 'json':
                fields_dict = json.loads(request.data.decode(charset))
            else:
                raise ValueError('unsupported format')

            if fields_dict is None:
                raise ValueError('fields are None')

            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            rpc_req = PutDocumentRequest()
            rpc_req.index_name = index_name
            rpc_req.doc_id = doc_id
            rpc_req.fields = pickle.dumps(fields_dict)
            rpc_req.sync = sync

            rpc_resp = self.__index_stub.PutDocument(rpc_req)

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

    def __get_document(self, index_name, doc_id):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, response)
            self.__record_http_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            rpc_req = GetDocumentRequest()
            rpc_req.index_name = index_name
            rpc_req.doc_id = doc_id

            rpc_resp = self.__index_stub.GetDocument(rpc_req)

            if rpc_resp.status.success:
                data['fields'] = pickle.loads(rpc_resp.fields)
                status_code = HTTPStatus.OK
            else:
                data['error'] = '{0}'.format(rpc_resp.status.message)
                if '{0} does not exist in {1}'.format(rpc_req.doc_id, rpc_req.index_name) == rpc_resp.status.message:
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

    def __delete_document(self, index_name, doc_id):
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

            rpc_req = DeleteDocumentRequest()
            rpc_req.index_name = index_name
            rpc_req.doc_id = doc_id
            rpc_req.sync = sync

            rpc_resp = self.__index_stub.DeleteDocument(rpc_req)

            if sync:
                if rpc_resp.status.success:
                    status_code = HTTPStatus.OK
                else:
                    data['error'] = '{0}'.format(rpc_resp.status.message)
                    if '{0} does not exist in {1}'.format(rpc_req.doc_id,
                                                          rpc_req.index_name) == rpc_resp.status.message:
                        status_code = HTTPStatus.NOT_FOUND
                    else:
                        status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            else:
                status_code = HTTPStatus.ACCEPTED
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

    def __put_documents(self, index_name):
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
                docs_dict = yaml.safe_load(request.data.decode(charset))
            elif mime[1] == 'json':
                docs_dict = json.loads(request.data.decode(charset))
            else:
                raise ValueError('unsupported format')

            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            rpc_req = PutDocumentsRequest()
            rpc_req.index_name = index_name
            rpc_req.docs = pickle.dumps(docs_dict)
            rpc_req.sync = sync

            rpc_resp = self.__index_stub.PutDocuments(rpc_req)

            if sync:
                if rpc_resp.status.success:
                    data['count'] = rpc_resp.count
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

    def __delete_documents(self, index_name):
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
                doc_ids_list = yaml.safe_load(request.data.decode(charset))
            elif mime[1] == 'json':
                doc_ids_list = json.loads(request.data.decode(charset))
            else:
                raise ValueError('unsupported format')

            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            rpc_req = DeleteDocumentsRequest()
            rpc_req.index_name = index_name
            rpc_req.doc_ids = pickle.dumps(doc_ids_list)
            rpc_req.sync = True

            rpc_resp = self.__index_stub.DeleteDocuments(rpc_req)

            if sync:
                if rpc_resp.status.success:
                    data['count'] = rpc_resp.count
                    status_code = HTTPStatus.OK
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

    def __search_documents(self, index_name):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, resp)
            self.__record_http_metrics(start_time, request, resp)
            return response

        data = {}
        status_code = None

        try:
            rpc_req = SearchDocumentsRequest()
            rpc_req.index_name = index_name
            rpc_req.query = request.args.get('query', default='', type=str)
            rpc_req.search_field = request.args.get('search_field', default='', type=str)
            rpc_req.page_num = request.args.get('page_num', default=1, type=int)
            rpc_req.page_len = request.args.get('page_len', default=10, type=int)
            if len(request.data) > 0:
                mime = mimeparse.parse_mime_type(request.headers.get('Content-Type'))
                charset = 'utf-8' if mime[2].get('charset') is None else mime[2].get('charset')
                if mime[1] == 'yaml':
                    rpc_req.weighting = pickle.dumps(yaml.safe_load(request.data.decode(charset)))
                elif mime[1] == 'json':
                    rpc_req.weighting = pickle.dumps(json.loads(request.data.decode(charset)))
                else:
                    raise ValueError('unsupported format')

            rpc_resp = self.__index_stub.SearchDocuments(rpc_req)

            if rpc_resp.status.success:
                data['results'] = pickle.loads(rpc_resp.results)
                status_code = HTTPStatus.OK
            else:
                data['error'] = '{0}'.format(rpc_resp.status.message)
                status_code = HTTPStatus.INTERNAL_SERVER_ERROR
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

    def __put_node(self, node_name):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, resp)
            self.__record_http_metrics(start_time, request, resp)
            return response

        data = {}
        status_code = None
        try:
            rpc_req = PutNodeRequest()
            rpc_req.node_name = node_name

            rpc_resp = self.__index_stub.PutNode(rpc_req)

            if rpc_resp.status.success:
                status_code = HTTPStatus.OK
            else:
                data['error'] = '{0}'.format(rpc_resp.status.message)
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

    def __get_status(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, resp)
            self.__record_http_metrics(start_time, request, resp)
            return response

        data = {}
        status_code = None

        try:
            rpc_resp = self.__index_stub.GetStatus(GetStatusRequest())

            if rpc_resp.status.success:
                data['node_status'] = pickle.loads(rpc_resp.node_status)
                status_code = HTTPStatus.OK
            else:
                data['error'] = '{0}'.format(rpc_resp.status.message)
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

    def __delete_node(self, node_name):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, resp)
            self.__record_http_metrics(start_time, request, resp)
            return response

        data = {}
        status_code = None

        try:
            rpc_req = DeleteNodeRequest()
            rpc_req.node_name = node_name

            rpc_resp = self.__index_stub.DeleteNode(rpc_req)

            if rpc_resp.status.success:
                status_code = HTTPStatus.OK
            else:
                data['error'] = '{0}'.format(rpc_resp.status.message)
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

    def __put_snapshot(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, resp)
            self.__record_http_metrics(start_time, request, resp)
            return response

        data = {}
        status_code = None

        try:
            rpc_req = CreateSnapshotRequest()

            rpc_resp = self.__index_stub.CreateSnapshot(rpc_req)

            if rpc_resp.status.success:
                status_code = HTTPStatus.ACCEPTED
            else:
                data['error'] = '{0}'.format(rpc_resp.status.message)
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

    def __get_snapshot(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, response)
            self.__record_http_metrics(start_time, request, response)
            return response

        try:
            rpc_req = SnapshotExistsRequest()
            rpc_resp = self.__index_stub.SnapshotExists(rpc_req)

            if rpc_resp.status.success:
                if rpc_resp.exist:
                    rpc_req = GetSnapshotRequest()
                    rpc_req.chunk_size = 1024

                    rpc_resp = self.__index_stub.GetSnapshot(rpc_req)

                    def generate():
                        for snapshot in rpc_resp:
                            yield snapshot.chunk

                    resp = Response(generate(), status=HTTPStatus.OK, mimetype='application/zip', headers={
                        'Content-Disposition': 'attachment; filename=snapshot.zip'
                    })
                else:
                    resp = Response(status=HTTPStatus.NOT_FOUND)
            else:
                resp = Response(status=HTTPStatus.INTERNAL_SERVER_ERROR)
        except Exception as ex:
            resp = Response(status=HTTPStatus.INTERNAL_SERVER_ERROR)
            self.__logger.error(ex)

        return resp

    def __healthiness(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, response)
            self.__record_http_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            rpc_req = IsHealthyRequest()

            rpc_resp = self.__index_stub.IsHealthy(rpc_req)

            if rpc_resp.status.success:
                data['healthy'] = rpc_resp.healthy
                if rpc_resp.healthy:
                    status_code = HTTPStatus.OK
                else:
                    status_code = HTTPStatus.SERVICE_UNAVAILABLE
                    data['error'] = '{0}'.format(rpc_resp.status.message)
            else:
                data['error'] = '{0}'.format(rpc_resp.status.message)
                status_code = HTTPStatus.INTERNAL_SERVER_ERROR
        except Exception as ex:
            data['healthy'] = False
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

    def __liveness(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, response)
            self.__record_http_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            rpc_req = IsAliveRequest()

            rpc_resp = self.__index_stub.IsAlive(rpc_req)

            if rpc_resp.status.success:
                data['liveness'] = rpc_resp.alive
                if rpc_resp.alive:
                    status_code = HTTPStatus.OK
                else:
                    status_code = HTTPStatus.SERVICE_UNAVAILABLE
                    data['error'] = '{0}'.format(rpc_resp.status.message)
            else:
                data['error'] = '{0}'.format(rpc_resp.status.message)
                status_code = HTTPStatus.INTERNAL_SERVER_ERROR
        except Exception as ex:
            data['liveness'] = False
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

    def __readiness(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, response)
            self.__record_http_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            rpc_req = IsReadyRequest()

            rpc_resp = self.__index_stub.IsReady(rpc_req)

            if rpc_resp.status.success:
                data['readiness'] = rpc_resp.ready
                if rpc_resp.ready:
                    status_code = HTTPStatus.OK
                else:
                    status_code = HTTPStatus.SERVICE_UNAVAILABLE
                    data['error'] = '{0}'.format(rpc_resp.status.message)
            else:
                data['error'] = '{0}'.format(rpc_resp.status.message)
                status_code = HTTPStatus.INTERNAL_SERVER_ERROR
        except Exception as ex:
            data['readiness'] = False
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

    def __metrics(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, response)
            self.__record_http_metrics(start_time, request, response)
            return response

        resp = Response()
        try:
            resp.status_code = HTTPStatus.OK
            resp.content_type = CONTENT_TYPE_LATEST
            resp.data = generate_latest(self.__metrics_registry)
        except Exception as ex:
            resp.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            resp.content_type = 'text/plain; charset="UTF-8"'
            resp.data = '{0}\n{1}'.format(resp.status_code.phrase, resp.status_code.description)
            self.__logger.error(ex)

        return resp

    def get_test_client(self, ues_cookies=True, **kwargs):
        return self.__app.test_client(ues_cookies, **kwargs)


def make_response(data, output='json'):
    resp = Response()

    if output == 'json':
        resp.data = json.dumps(data, indent=2)
        resp.content_type = 'application/json; charset="UTF-8"'
    elif output == 'yaml':
        resp.data = yaml.safe_dump(data, default_flow_style=False, indent=2)
        resp.content_type = 'application/yaml; charset="UTF-8"'

    return resp
