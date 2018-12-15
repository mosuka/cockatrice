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
from whoosh.scoring import BM25F

from cockatrice import NAME, VERSION
from cockatrice.schema import Schema
from cockatrice.scoring import MultiWeighting

TRUE_STRINGS = ['true', 'yes', 'on', 't', 'y', '1']


class IndexHTTPServer:
    def __init__(self, index_server, port=8080, logger=getLogger(NAME),
                 http_logger=getLogger(NAME + '_http'), metrics_registry=CollectorRegistry()):
        self.__logger = logger
        self.__http_logger = http_logger
        self.__metrics_registry = metrics_registry

        self.__port = port
        self.__index_server = index_server

        self.__app = Flask('index_http_server')
        self.__app.add_url_rule('/', endpoint='get_node', view_func=self.__get_node, methods=['GET'])
        self.__app.add_url_rule('/indices/<index_name>', endpoint='get_index', view_func=self.__get_index, methods=['GET'])
        self.__app.add_url_rule('/indices/<index_name>', endpoint='create_index', view_func=self.__create_index, methods=['PUT'])
        self.__app.add_url_rule('/indices/<index_name>', endpoint='delete_index', view_func=self.__delete_index, methods=['DELETE'])
        self.__app.add_url_rule('/indices/<index_name>/documents/<doc_id>', endpoint='get_document', view_func=self.__get_document, methods=['GET'])
        self.__app.add_url_rule('/indices/<index_name>/documents/<doc_id>', endpoint='index_document', view_func=self.__index_document, methods=['PUT'])
        self.__app.add_url_rule('/indices/<index_name>/documents/<doc_id>', endpoint='delete_document', view_func=self.__delete_document, methods=['DELETE'])
        self.__app.add_url_rule('/indices/<index_name>/documents', endpoint='index_documents', view_func=self.__index_documents, methods=['PUT'])
        self.__app.add_url_rule('/indices/<index_name>/documents', endpoint='delete_documents', view_func=self.__delete_documents, methods=['DELETE'])
        self.__app.add_url_rule('/indices/<index_name>/search', endpoint='search_documents', view_func=self.__search_documents, methods=['GET', 'POST'])
        self.__app.add_url_rule('/indices/<index_name>/optimize', endpoint='optimize_index', view_func=self.__optimize_index, methods=['GET'])
        # self.__app.add_url_rule('/cluster/<node_name>', endpoint='get_node', view_func=self.__get_node, methods=['GET'])
        # self.__app.add_url_rule('/cluster/<node_name>', endpoint='get_node', view_func=self.__get_node, methods=['PUT'])
        # self.__app.add_url_rule('/cluster/<node_name>', endpoint='get_node', view_func=self.__get_node, methods=['DELETEW'])
        self.__app.add_url_rule('/health/liveness', endpoint='liveness', view_func=self.__liveness, methods=['GET'])
        self.__app.add_url_rule('/health/readiness', endpoint='readiness', view_func=self.__readiness, methods=['GET'])
        self.__app.add_url_rule('/metrics', endpoint='metrics', view_func=self.__metrics, methods=['GET'])

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

    def start(self):
        try:
            # run server
            self.__logger.info('starting index http server')
            self.__app.run(host='0.0.0.0', port=self.__port)
        except OSError as ex:
            self.__logger.critical(ex)
        except Exception as ex:
            self.__logger.critical(ex)

    def stop(self):
        self.__logger.info('stopping index http server')

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

        # make response
        resp = jsonify(data)
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

            use_ram_storage = False
            if request.args.get('use_ram_storage', default='', type=str).lower() in TRUE_STRINGS:
                use_ram_storage = True

            index = self.__index_server.create_index(index_name, Schema(request.data), use_ram_storage=use_ram_storage,
                                                     sync=sync)

            if sync:
                if index is None:
                    raise ValueError('failed to create index')
                else:
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

        # make response
        resp = jsonify(data)
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

        # make response
        resp = jsonify(data)
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

        # make response
        resp = jsonify(data)
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

            fields = {}

            hit = results_page.results[0]
            for i in hit.iteritems():
                fields[i[0]] = i[1]

            doc = {'fields': fields}
            data['doc'] = doc

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

        # make response
        resp = jsonify(data)
        resp.status_code = status_code

        return resp

    def __index_document(self, index_name, doc_id):
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

            self.__index_server.index_document(index_name, doc_id, json.loads(request.data, encoding='utf-8'),
                                               sync=sync)

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

        # make response
        resp = jsonify(data)
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

        # make response
        resp = jsonify(data)
        resp.status_code = status_code

        return resp

    def __index_documents(self, index_name):
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

            cnt = self.__index_server.index_documents(index_name, json.loads(request.data), sync=sync)
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

        # make response
        resp = jsonify(data)
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

            cnt = self.__index_server.delete_documents(index_name, json.loads(request.data), sync=sync)
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

        # make response
        resp = jsonify(data)
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
            weighting = BM25F
            if len(request.data) > 0:
                try:
                    weighting = MultiWeighting(request.data)
                except Exception as ex:
                    self.__logger.error('failed to load weighting config: {0}'.format(ex))

            query = request.args.get('query', default='', type=str)

            search_field = request.args.get('search_field', default=self.__index_server.get_index(
                index_name).schema.get_default_search_field(), type=str)
            page_num = request.args.get('page_num', default=1, type=int)
            page_len = request.args.get('page_len', default=10, type=int)

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

        # make response
        resp = jsonify(data)
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

        # make response
        resp = jsonify(data)
        resp.status_code = status_code

        return resp

    # def __join(self):
    #     start_time = time.time()
    #
    #     @after_this_request
    #     def to_do_after_this_request(response):
    #         return self.__post_process(start_time, request, response)
    #
    #     data = {}
    #     status_code = None
    #
    #     try:
    #         node = request.args.get('node', default='', type=str)
    #
    #         self.__index_server.addNodeToCluster(node)
    #
    #         status_code = HTTPStatus.OK
    #     except Exception as ex:
    #         data['error'] = '{0}'.format(ex.args[0])
    #         status_code = HTTPStatus.INTERNAL_SERVER_ERROR
    #         self.__logger.error(ex)
    #     finally:
    #         data['time'] = time.time() - start_time
    #         data['status'] = {'code': status_code.value, 'phrase': status_code.phrase,
    #                           'description': status_code.description}
    #
    #     resp = jsonify(data)
    #     resp.status_code = status_code
    #
    #     return resp

    # def __leave(self):
    #     start_time = time.time()
    #
    #     @after_this_request
    #     def to_do_after_this_request(response):
    #         return self.__post_process(start_time, request, response)
    #
    #     data = {}
    #     status_code = None
    #
    #     try:
    #         node = request.args.get('node', default='', type=str)
    #
    #         self.__index_server.removeNodeFromCluster(node)
    #
    #         data['cluster_status'] = self.__index_server.getStatus()
    #
    #         status_code = HTTPStatus.OK
    #     except Exception as ex:
    #         data['error'] = '{0}'.format(ex.args[0])
    #         status_code = HTTPStatus.INTERNAL_SERVER_ERROR
    #         self.__logger.error(ex)
    #     finally:
    #         data['time'] = time.time() - start_time
    #         data['status'] = {'code': status_code.value, 'phrase': status_code.phrase,
    #                           'description': status_code.description}
    #
    #     resp = jsonify(data)
    #     resp.status_code = status_code
    #
    #     return resp

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

        # make response
        resp = jsonify(data)
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

        # make response
        resp = jsonify(data)
        resp.status_code = status_code

        return resp

    def get_test_client(self, ues_cookies=True, **kwargs):
        return self.__app.test_client(ues_cookies, **kwargs)
