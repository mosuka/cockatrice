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
from threading import Thread

import yaml
from flask import after_this_request, Flask, request, Response
from prometheus_client.core import CollectorRegistry, Counter, Gauge, Histogram
from prometheus_client.exposition import CONTENT_TYPE_LATEST, generate_latest
from werkzeug.serving import make_server
from whoosh.scoring import BM25F

from cockatrice import NAME, VERSION
from cockatrice.schema import Schema
from cockatrice.scoring import get_multi_weighting

TRUE_STRINGS = ['true', 'yes', 'on', 't', 'y', '1']


class ServerThread(Thread):
    def __init__(self, host, port, app, logger=getLogger()):
        self.__logger = logger
        self.__logger.info('creating server thread')

        Thread.__init__(self)
        self.srv = make_server(host, port, app)
        self.ctx = app.app_context()
        self.ctx.push()

    def run(self):
        self.__logger.info('starting server thread')
        self.srv.serve_forever()

    def shutdown(self):
        self.__logger.info('stopping server thread')
        self.srv.shutdown()


class IndexHTTPServer:
    def __init__(self, index_server, host='localhost', port=8080, logger=getLogger(), http_logger=getLogger(),
                 metrics_registry=CollectorRegistry()):
        self.__logger = logger
        self.__http_logger = http_logger
        self.__metrics_registry = metrics_registry

        self.__host = host
        self.__port = port
        self.__index_server = index_server

        self.__logger.info('starting index http server: {0}'.format(self.__port))

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
        self.__app.add_url_rule('/cluster', endpoint='get_cluster', view_func=self.__get_cluster, methods=['GET'])
        self.__app.add_url_rule('/cluster/<node_name>', endpoint='put_node', view_func=self.__put_node, methods=['PUT'])
        self.__app.add_url_rule('/cluster/<node_name>', endpoint='delete_node', view_func=self.__delete_node,
                                methods=['DELETE'])
        self.__app.add_url_rule('/metrics', endpoint='metrics', view_func=self.__metrics, methods=['GET'])
        self.__app.add_url_rule('/health/liveness', endpoint='liveness', view_func=self.__liveness, methods=['GET'])
        self.__app.add_url_rule('/health/readiness', endpoint='readiness', view_func=self.__readiness, methods=['GET'])
        self.__app.add_url_rule('/snapshot', endpoint='get_snapshot', view_func=self.__get_snapshot, methods=['GET'])
        self.__app.add_url_rule('/snapshot', endpoint='put_snapshot', view_func=self.__put_snapshot, methods=['PUT'])

        # disable Flask default logger
        self.__app.logger.disabled = True
        getLogger('werkzeug').disabled = True

        # metrics
        self.__metrics_http_requests_total = Counter(
            '{0}_http_requests_total'.format(NAME),
            'The number of requests.',
            [
                'method',
                'endpoint',
                'status_code'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_http_requests_bytes_total = Counter(
            '{0}_http_requests_bytes_total'.format(NAME),
            'A summary of the invocation requests bytes.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_http_responses_bytes_total = Counter(
            '{0}_http_responses_bytes_total'.format(NAME),
            'A summary of the invocation responses bytes.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_http_requests_duration_seconds = Histogram(
            '{0}_http_requests_duration_seconds'.format(NAME),
            'The invocation duration in seconds.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_index_documents = Gauge(
            '{0}_index_documents'.format(NAME),
            'The number of documents.',
            [
                'index_name',
            ],
            registry=self.__metrics_registry
        )

        self.__server_thread = None
        try:
            # run server
            self.__server_thread = ServerThread(self.__host, self.__port, self.__app, logger=self.__logger)
            self.__server_thread.start()
            self.__logger.info('index http server started')
        except Exception as ex:
            self.__logger.critical(ex)

    def stop(self):
        self.__logger.info('stopping index http server')
        self.__server_thread.shutdown()

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

    def __record_index_metrics(self, index_name):
        doc_count = self.__index_server.get_doc_count(index_name)
        if doc_count is not None:
            self.__metrics_index_documents.labels(
                index_name=index_name,
            ).set(doc_count)
        else:
            self.__metrics_index_documents.labels(
                index_name=index_name,
            ).set(0)

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
            index = self.__index_server.get_index(index_name)
            if index is None:
                raise KeyError('index does not exist')

            data['index'] = {}
            data['index']['name'] = index_name
            data['index']['doc_count'] = index.doc_count()
            data['index']['doc_count_all'] = index.doc_count_all()
            data['index']['last_modified'] = index.last_modified()
            data['index']['latest_generation'] = index.latest_generation()
            data['index']['version'] = index.version
            data['index']['storage'] = {
                'folder': index.storage.folder,
                'supports_mmap': index.storage.supports_mmap,
                'readonly': index.storage.readonly,
                'files': index.storage.list()
            }
            status_code = HTTPStatus.OK
        except KeyError as ex:
            data['error'] = '{0}'.format(ex.args[0])
            status_code = HTTPStatus.NOT_FOUND
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

    def __create_index(self, index_name):
        start_time = time.time()

        data = {}
        status_code = None

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, response)
            self.__record_http_metrics(start_time, request, response)
            return response

        try:
            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            try:
                if 'application/yaml' in request.headers.get('Content-Type'):
                    schema = Schema(yaml.safe_load(request.data.decode("utf-8")))
                elif 'application/json' in request.headers.get('Content-Type'):
                    schema = Schema(json.loads(request.data.decode("utf-8")))
                else:
                    raise ValueError('unsupported Content-Type')
            except Exception as ex:
                raise ex

            index = self.__index_server.create_index(index_name, schema, sync=sync)

            if sync:
                if index is None:
                    raise ValueError('failed to create index')
                else:
                    status_code = HTTPStatus.CREATED
            else:
                status_code = HTTPStatus.ACCEPTED
        except (json.decoder.JSONDecodeError, ValueError) as ex:
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

            self.__index_server.delete_index(index_name, sync=sync)

            if sync:
                status_code = HTTPStatus.OK
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

            self.__index_server.optimize_index(index_name, sync=sync)

            if sync:
                status_code = HTTPStatus.OK
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
            results_page = self.__index_server.get_document(index_name, doc_id)

            if results_page.total <= 0:
                raise KeyError('{0} does not exist in {1}'.format(doc_id, index_name))

            hit = results_page.results[0]
            fields = {}
            for i in hit.iteritems():
                fields[i[0]] = i[1]
            data['fields'] = fields

            status_code = HTTPStatus.OK
        except KeyError as ex:
            data['error'] = '{0}'.format(ex.args[0])
            status_code = HTTPStatus.NOT_FOUND
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
            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            try:
                if 'application/yaml' in request.headers.get('Content-Type'):
                    print(request.get_data(as_text=False, parse_form_data=True))
                    doc = yaml.safe_load(request.data.decode("utf-8"))
                elif 'application/json' in request.headers.get('Content-Type'):
                    doc = json.loads(request.data.decode("utf-8"))
                else:
                    raise ValueError('unsupported Content-Type')
            except Exception as ex:
                raise ex

            self.__index_server.put_document(index_name, doc_id, doc, sync=sync)

            if sync:
                status_code = HTTPStatus.CREATED
            else:
                status_code = HTTPStatus.ACCEPTED
        except json.decoder.JSONDecodeError as ex:
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

            self.__index_server.delete_document(index_name, doc_id, sync=sync)

            if sync:
                status_code = HTTPStatus.OK
            else:
                status_code = HTTPStatus.ACCEPTED
        except KeyError as ex:
            data['error'] = '{0}'.format(ex.args[0])
            status_code = HTTPStatus.NOT_FOUND
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
            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            try:
                if 'application/yaml' in request.headers.get('Content-Type'):
                    docs = yaml.safe_load(request.data)
                elif 'application/json' in request.headers.get('Content-Type'):
                    docs = json.loads(request.data)
                else:
                    raise ValueError('unsupported Content-Type')
            except Exception as ex:
                raise ex

            cnt = self.__index_server.put_documents(index_name, docs, sync=sync)
            if cnt is not None:
                data['count'] = cnt

            if sync:
                status_code = HTTPStatus.CREATED
            else:
                status_code = HTTPStatus.ACCEPTED
        except json.decoder.JSONDecodeError as ex:
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
            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            try:
                if 'application/yaml' in request.headers.get('Content-Type'):
                    doc_ids = yaml.safe_load(request.data)
                elif 'application/json' in request.headers.get('Content-Type'):
                    doc_ids = json.loads(request.data)
                else:
                    raise ValueError('unsupported Content-Type')
            except Exception as ex:
                raise ex

            cnt = self.__index_server.delete_documents(index_name, doc_ids, sync=sync)
            if cnt is not None:
                data['count'] = cnt

            if sync:
                status_code = HTTPStatus.OK
            else:
                status_code = HTTPStatus.ACCEPTED
        except KeyError as ex:
            data['error'] = '{0}'.format(ex.args[0])
            status_code = HTTPStatus.NOT_FOUND
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
            query = request.args.get('query', default='', type=str)
            search_field = request.args.get('search_field', default=self.__index_server.get_index(
                index_name).schema.get_default_search_field(), type=str)
            page_num = request.args.get('page_num', default=1, type=int)
            page_len = request.args.get('page_len', default=10, type=int)
            weighting = BM25F
            if len(request.data) > 0:
                try:
                    if 'application/yaml' in request.headers.get('Content-Type'):
                        weighting = get_multi_weighting(yaml.safe_load(request.data))
                    elif 'application/json' in request.headers.get('Content-Type'):
                        weighting = get_multi_weighting(json.loads(request.data))
                    else:
                        raise ValueError('unsupported Content-Type')
                except Exception as ex:
                    self.__logger.error('failed to load weighting config: {0}'.format(ex))

            results_page = self.__index_server.search_documents(index_name, query, search_field, page_num,
                                                                page_len=page_len, weighting=weighting)

            data['results'] = {
                'is_last_page': results_page.is_last_page(),
                'page_count': results_page.pagecount,
                'page_len': results_page.pagelen,
                'page_num': results_page.pagenum,
                'total': results_page.total
            }
            if results_page.pagecount >= page_num or results_page.total <= 0:
                hits = []
                for result in results_page.results[results_page.offset:]:
                    doc = {'fields': {}}
                    for item in result.iteritems():
                        doc['fields'][item[0]] = item[1]
                    hit = {
                        'doc': doc,
                        'score': result.score,
                        'rank': result.rank,
                        'pos': result.pos
                    }
                    hits.append(hit)
                data['results']['hits'] = hits
            else:
                raise ValueError('page_num must be <= {0}'.format(results_page.pagecount))

            status_code = HTTPStatus.OK
        except ValueError as ex:
            data['error'] = '{0}'.format(ex.args[0])
            status_code = HTTPStatus.MISDIRECTED_REQUEST
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

    def __get_node(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, resp)
            self.__record_http_metrics(start_time, request, resp)
            return response

        data = {}
        status_code = None
        try:
            data['node'] = self.__index_server.getStatus()
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

    def __get_cluster(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, resp)
            self.__record_http_metrics(start_time, request, resp)
            return response

        data = {}
        status_code = None
        try:
            data['cluster'] = self.__index_server.getStatus()
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
            self.__index_server.addNodeToCluster(node_name)

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
            self.__index_server.removeNodeFromCluster(node_name)

            data['cluster'] = self.__index_server.getStatus()

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

    def __metrics(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, response)
            self.__record_http_metrics(start_time, request, response)
            return response

        # index metrics
        for index_name in self.__index_server.get_indices().keys():
            self.__record_index_metrics(index_name)

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
            data['liveness'] = True
            status_code = HTTPStatus.OK
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
            if self.__index_server.isReady():
                data['readiness'] = True
                status_code = HTTPStatus.OK
            else:
                raise Exception('index server is not ready')
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

    def __get_snapshot(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            self.__record_http_log(request, response)
            self.__record_http_metrics(start_time, request, response)
            return response

        resp = Response()
        try:
            resp.status_code = HTTPStatus.OK
            resp.headers['Content-Type'] = 'application/zip'
            with self.__index_server.open_snapshot_file() as f:
                resp.headers['Content-Disposition'] = 'attachment; filename=' + f.name
                resp.data = f.read()
        except FileNotFoundError as ex:
            resp.status_code = HTTPStatus.NOT_FOUND
            resp.content_type = 'text/plain; charset="UTF-8"'
            resp.data = '{0}\n{1}'.format(resp.status_code.phrase, resp.status_code.description)
            self.__logger.error(ex)
        except Exception as ex:
            resp.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            resp.content_type = 'text/plain; charset="UTF-8"'
            resp.data = '{0}\n{1}'.format(resp.status_code.phrase, resp.status_code.description)
            self.__logger.error(ex)

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
            self.__index_server.forceLogCompaction()

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
