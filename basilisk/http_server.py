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

TRUE_STRINGS = ['true', 'yes', 'on', 't', 'y', '1']


class HTTPServer:
    def __init__(self, name, port, data_node, schema, logger=getLogger(APP_NAME),
                 http_logger=getLogger(APP_NAME + '_http'), metrics_registry=CollectorRegistry()):
        self.__logger = logger
        self.__http_logger = http_logger
        self.__metrics_registry = metrics_registry

        self.__port = port
        self.__data_node = data_node
        self.__schema = schema

        self.__app = Flask(name)
        self.__app.add_url_rule('/rest/doc/<doc_id>', 'get', self.__get, methods=['GET'])
        self.__app.add_url_rule('/rest/doc/<doc_id>', 'index', self.__index, methods=['PUT'])
        self.__app.add_url_rule('/rest/doc/<doc_id>', 'delete', self.__delete, methods=['DELETE'])
        self.__app.add_url_rule('/rest/bulk', 'bulk_index', self.__bulk_index, methods=['PUT'])
        self.__app.add_url_rule('/rest/bulk', 'bulk_delete', self.__bulk_delete, methods=['DELETE'])
        self.__app.add_url_rule('/rest/search', 'search', self.__search, methods=['GET'])
        self.__app.add_url_rule('/metrics', 'metrics', self.__metrics, methods=['GET'])

        # disable Flask default logger
        self.__app.logger.disabled = True
        getLogger('werkzeug').disabled = True

        # metrics
        self.__metrics_http_requests_total = Counter(
            'http_requests_total',
            'The number of requests.',
            [
                'method',
                'endpoint',
                'status_code'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_http_requests_bytes_total = Counter(
            'http_requests_bytes_total',
            'A summary of the invocation requests bytes.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_http_responses_bytes_total = Counter(
            'http_responses_bytes_total',
            'A summary of the invocation responses bytes.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_http_requests_duration_seconds = Histogram(
            'http_requests_duration_seconds',
            'The invocation duration in seconds.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )
        # self.__metrics_kvs_records_count = Gauge(
        #     'kvs_records_count',
        #     'The number of kvs records.',
        #     registry=self.__metrics_registry
        # )

    def start(self):
        try:
            self.__app.run(host='0.0.0.0', port=self.__port)
        except OSError as ex:
            self.__logger.critical(ex)
        except Exception as ex:
            self.__logger.critical(ex)

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

    def __record_metrics(self, start_time, req, resp):
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

        # self.metrics_kvs_records_count.set(self.data_node.len())

        return

    def __post_process(self, start_time, req, resp):
        self.__record_http_log(req, resp)
        self.__record_metrics(start_time, req, resp)

        return resp

    def __get(self, doc_id):
        start_time = time.time()

        data = {}
        status_code = None

        @after_this_request
        def to_do_after_this_request(response):
            return self.__post_process(start_time, request, response)

        try:
            results_page = self.__data_node.get(doc_id)

            if results_page.total <= 0:
                raise KeyError('{0} does not exist'.format(doc_id))

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

        resp = jsonify(data)
        resp.status_code = status_code

        return resp

    def __index(self, doc_id):
        start_time = time.time()

        data = {}
        status_code = None

        @after_this_request
        def to_do_after_this_request(response):
            return self.__post_process(start_time, request, response)

        try:
            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            self.__data_node.index(doc_id, json.loads(request.data), sync=sync)

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

        resp = jsonify(data)
        resp.status_code = status_code

        return resp

    def __delete(self, doc_id):
        start_time = time.time()

        data = {}
        status_code = None

        @after_this_request
        def to_do_after_this_request(response):
            return self.__post_process(start_time, request, response)

        try:
            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            self.__data_node.delete(doc_id, sync=sync)

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

        resp = jsonify(data)
        resp.status_code = status_code

        return resp

    def __bulk_index(self):
        start_time = time.time()

        data = {}
        status_code = None

        @after_this_request
        def to_do_after_this_request(response):
            return self.__post_process(start_time, request, response)

        try:
            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            cnt = self.__data_node.bulk_index(json.loads(request.data), sync=sync)
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

        resp = jsonify(data)
        resp.status_code = status_code

        return resp

    def __bulk_delete(self):
        start_time = time.time()

        data = {}
        status_code = None

        @after_this_request
        def to_do_after_this_request(response):
            return self.__post_process(start_time, request, response)

        try:
            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            cnt = self.__data_node.bulk_delete(json.loads(request.data), sync=sync)
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

        resp = jsonify(data)
        resp.status_code = status_code

        return resp

    def __search(self):
        start_time = time.time()

        data = {}
        results = {}
        status_code = None

        @after_this_request
        def to_do_after_this_request(response):
            return self.__post_process(start_time, request, response)

        try:
            query = request.args.get('query', default='', type=str)
            search_field = request.args.get('search_field', default=self.__schema.get_default_search_field(), type=str)
            page_num = request.args.get('page_num', default=1, type=int)
            page_len = request.args.get('page_len', default=10, type=int)

            results_page = self.__data_node.search(query, search_field, page_num, page_len=page_len)

            results['is_last_page'] = results_page.is_last_page()
            results['page_count'] = results_page.pagecount
            results['page_len'] = results_page.pagelen
            results['page_num'] = results_page.pagenum
            results['total'] = results_page.total

            if results_page.pagecount >= page_num or results_page.total <= 0:
                hits = []
                for hit in results_page.results[results_page.offset:]:
                    doc = {'fields': {}}
                    for item in hit.iteritems():
                        doc['fields'][item[0]] = item[1]

                    h = {
                        'doc': doc,
                        'score': hit.score,
                        'rank': hit.rank,
                        'pos': hit.pos
                    }

                    hits.append(h)
                results['hits'] = hits
            else:
                raise ValueError('page_num must be <= {0}'.format(results_page.pagecount))

            data['results'] = results

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

        resp = jsonify(data)
        resp.status_code = status_code

        return resp

    def __metrics(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            return self.__post_process(start_time, request, response)

        resp = Response()
        status_code = None

        try:
            resp.data = generate_latest(self.__metrics_registry)
            status_code = HTTPStatus.OK
        except Exception as ex:
            resp.data = '{0}\n{1}'.format(status_code.phrase, status_code.description)
            status_code = HTTPStatus.INTERNAL_SERVER_ERROR
            self.__logger.error(ex)

        resp.status_code = status_code
        resp.content_type = CONTENT_TYPE_LATEST

        return resp

    def get_test_client(self, ues_cookies=True, **kwargs):
        return self.__app.test_client(ues_cookies, **kwargs)