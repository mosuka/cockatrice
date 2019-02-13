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
from flask import after_this_request, Flask, request, Response
from prometheus_client.core import CollectorRegistry, Counter, Histogram
from prometheus_client.exposition import CONTENT_TYPE_LATEST, generate_latest
from whoosh.scoring import BM25F
from yaml.constructor import ConstructorError

from cockatrice import NAME, VERSION
from cockatrice.index_config import IndexConfig
from cockatrice.scoring import get_multi_weighting
from cockatrice.util.http import make_response, record_log, TRUE_STRINGS


class IndexHTTPServicer:
    def __init__(self, indexer, logger=getLogger(), http_logger=getLogger(),
                 metrics_registry=CollectorRegistry()):
        self.__indexer = indexer
        self.__logger = logger
        self.__http_logger = http_logger
        self.__metrics_registry = metrics_registry

        # metrics
        self.__metrics_requests_total = Counter(
            '{0}_indexer_http_requests_total'.format(NAME),
            'The number of requests.',
            [
                'method',
                'endpoint',
                'status_code'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_requests_duration_seconds = Histogram(
            '{0}_indexer_http_requests_duration_seconds'.format(NAME),
            'The invocation duration in seconds.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_requests_bytes_total = Counter(
            '{0}_indexer_http_requests_bytes_total'.format(NAME),
            'A summary of the invocation requests bytes.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_responses_bytes_total = Counter(
            '{0}_indexer_http_responses_bytes_total'.format(NAME),
            'A summary of the invocation responses bytes.',
            [
                'method',
                'endpoint'
            ],
            registry=self.__metrics_registry
        )

        self.app = Flask('indexer_http')
        self.app.add_url_rule('/', endpoint='root', view_func=self.__root, methods=['GET'])
        self.app.add_url_rule('/indices/<index_name>', endpoint='get_index', view_func=self.__get_index,
                              methods=['GET'])
        self.app.add_url_rule('/indices/<index_name>', endpoint='create_index', view_func=self.__create_index,
                              methods=['PUT'])
        self.app.add_url_rule('/indices/<index_name>', endpoint='delete_index', view_func=self.__delete_index,
                              methods=['DELETE'])
        self.app.add_url_rule('/indices/<index_name>/documents/<doc_id>', endpoint='get_document',
                              view_func=self.__get_document, methods=['GET'])
        self.app.add_url_rule('/indices/<index_name>/documents/<doc_id>', endpoint='put_document',
                              view_func=self.__put_document, methods=['PUT'])
        self.app.add_url_rule('/indices/<index_name>/documents/<doc_id>', endpoint='delete_document',
                              view_func=self.__delete_document, methods=['DELETE'])
        self.app.add_url_rule('/indices/<index_name>/documents', endpoint='put_documents',
                              view_func=self.__put_documents, methods=['PUT'])
        self.app.add_url_rule('/indices/<index_name>/documents', endpoint='delete_documents',
                              view_func=self.__delete_documents, methods=['DELETE'])
        self.app.add_url_rule('/indices/<index_name>/search', endpoint='search_documents',
                              view_func=self.__search_documents, methods=['GET', 'POST'])
        self.app.add_url_rule('/indices/<index_name>/optimize', endpoint='optimize_index',
                              view_func=self.__optimize_index, methods=['GET'])
        self.app.add_url_rule('/indices/<index_name>/commit', endpoint='commit',
                              view_func=self.__commit_index, methods=['GET'])
        self.app.add_url_rule('/indices/<index_name>/rollback', endpoint='rollback',
                              view_func=self.__rollback_index, methods=['GET'])
        self.app.add_url_rule('/nodes/<node_name>', endpoint='put_node', view_func=self.__put_node, methods=['PUT'])
        self.app.add_url_rule('/nodes/<node_name>', endpoint='delete_node', view_func=self.__delete_node,
                              methods=['DELETE'])
        self.app.add_url_rule('/snapshot', endpoint='get_snapshot', view_func=self.__get_snapshot, methods=['GET'])
        self.app.add_url_rule('/snapshot', endpoint='put_snapshot', view_func=self.__put_snapshot, methods=['PUT'])
        self.app.add_url_rule('/metrics', endpoint='metrics', view_func=self.__metrics, methods=['GET'])
        self.app.add_url_rule('/healthiness', endpoint='healthiness', view_func=self.__healthiness, methods=['GET'])
        self.app.add_url_rule('/liveness', endpoint='liveness', view_func=self.__liveness, methods=['GET'])
        self.app.add_url_rule('/readiness', endpoint='readiness', view_func=self.__readiness, methods=['GET'])
        self.app.add_url_rule('/status', endpoint='status', view_func=self.__get_status, methods=['GET'])

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

    def __root(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, response)
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
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            mime = mimeparse.parse_mime_type(request.headers.get('Content-Type'))
            charset = 'utf-8' if mime[2].get('charset') is None else mime[2].get('charset')
            if mime[1] == 'yaml':
                index_config_dict = yaml.safe_load(request.data.decode(charset))
            elif mime[1] == 'json':
                index_config_dict = json.loads(request.data.decode(charset))
            else:
                raise ValueError('unsupported format')

            if index_config_dict is None:
                raise ValueError('index config is None')

            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            index_config = IndexConfig(index_config_dict)
            self.__indexer.create_index(index_name, index_config, sync=sync)

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

    def __get_index(self, index_name):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            index = self.__indexer.get_index(index_name)
            if index is None:
                status_code = HTTPStatus.NOT_FOUND
            else:
                data['index'] = {
                    'name': index.indexname,
                    'doc_count': index.doc_count(),
                    'doc_count_all': index.doc_count_all(),
                    'last_modified': index.latest_generation(),
                    'latest_generation': index.last_modified(),
                    'version': index.version,
                    'storage': {
                        'folder': index.storage.folder,
                        'supports_mmap': index.storage.supports_mmap,
                        'readonly': index.storage.readonly,
                        'files': list(index.storage.list())
                    }
                }
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

    def __delete_index(self, index_name):
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

            self.__indexer.delete_index(index_name, sync=sync)

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

    def __commit_index(self, index_name):
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

            self.__indexer.commit_index(index_name, sync=sync)

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

    def __rollback_index(self, index_name):
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

            self.__indexer.rollback_index(index_name, sync=sync)

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
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            self.__indexer.optimize_index(index_name, sync=sync)

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

    def __put_document(self, index_name, doc_id):
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

            count = self.__indexer.put_document(index_name, doc_id, fields_dict, sync=sync)

            if sync:
                if count > 0:
                    data['count'] = count
                    status_code = HTTPStatus.CREATED
                else:
                    status_code = HTTPStatus.INTERNAL_SERVER_ERROR
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

    def __get_document(self, index_name, doc_id):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            results_page = self.__indexer.get_document(index_name, doc_id)

            if results_page.total > 0:
                fields = {}
                for i in results_page.results[0].iteritems():
                    fields[i[0]] = i[1]
                data['fields'] = fields
                status_code = HTTPStatus.OK
            else:
                status_code = HTTPStatus.NOT_FOUND
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
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            count = self.__indexer.delete_document(index_name, doc_id, sync=sync)

            if sync:
                if count > 0:
                    status_code = HTTPStatus.OK
                elif count == 0:
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
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, response)
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

            count = self.__indexer.put_documents(index_name, docs_dict, sync=sync)

            if sync:
                if count > 0:
                    data['count'] = count
                    status_code = HTTPStatus.CREATED
                else:
                    status_code = HTTPStatus.INTERNAL_SERVER_ERROR
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

    def __delete_documents(self, index_name):
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
                doc_ids_list = yaml.safe_load(request.data.decode(charset))
            elif mime[1] == 'json':
                doc_ids_list = json.loads(request.data.decode(charset))
            else:
                raise ValueError('unsupported format')

            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            count = self.__indexer.delete_documents(index_name, doc_ids_list, sync=sync)

            if sync:
                if count > 0:
                    data['count'] = count
                    status_code = HTTPStatus.OK
                else:
                    status_code = HTTPStatus.INTERNAL_SERVER_ERROR
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

    def __search_documents(self, index_name):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, resp)
            return response

        data = {}
        status_code = None

        try:
            query = request.args.get('query', default='', type=str)
            search_field = request.args.get('search_field', default='', type=str)
            page_num = request.args.get('page_num', default=1, type=int)
            page_len = request.args.get('page_len', default=10, type=int)
            weighting = BM25F
            if len(request.data) > 0:
                mime = mimeparse.parse_mime_type(request.headers.get('Content-Type'))
                charset = 'utf-8' if mime[2].get('charset') is None else mime[2].get('charset')
                if mime[1] == 'yaml':
                    weighting = get_multi_weighting(yaml.safe_load(request.data.decode(charset)))
                elif mime[1] == 'json':
                    weighting = get_multi_weighting(json.loads(request.data.decode(charset)))
                else:
                    raise ValueError('unsupported format')

            results_page = self.__indexer.search_documents(index_name, query, search_field, page_num,
                                                           page_len=page_len, weighting=weighting)

            if results_page.pagecount >= page_num or results_page.total <= 0:
                results = {
                    'is_last_page': results_page.is_last_page(),
                    'page_count': results_page.pagecount,
                    'page_len': results_page.pagelen,
                    'page_num': results_page.pagenum,
                    'total': results_page.total,
                    'offset': results_page.offset
                }
                hits = []
                for result in results_page.results[results_page.offset:]:
                    fields = {}
                    for item in result.iteritems():
                        fields[item[0]] = item[1]
                    hit = {
                        'fields': fields,
                        'doc_num': result.docnum,
                        'score': result.score,
                        'rank': result.rank,
                        'pos': result.pos
                    }
                    hits.append(hit)
                results['hits'] = hits

                data['results'] = results
                status_code = HTTPStatus.OK
            else:
                data['error'] = 'page_num must be <= {0}'.format(results_page.pagecount)
                status_code = HTTPStatus.BAD_REQUEST
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

    def __put_node(self, node_name):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, resp)
            return response

        data = {}
        status_code = None

        try:
            self.__indexer.addNodeToCluster(node_name)

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

    def __get_status(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, resp)
            return response

        data = {}
        status_code = None

        try:
            data['node_status'] = self.__indexer.getStatus()
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
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, resp)
            return response

        data = {}
        status_code = None

        try:
            self.__indexer.removeNodeFromCluster(node_name)

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

    def __put_snapshot(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, resp)
            return response

        data = {}
        status_code = None

        try:
            sync = False
            if request.args.get('sync', default='', type=str).lower() in TRUE_STRINGS:
                sync = True

            self.__indexer.create_snapshot(sync=sync)

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

    def __get_snapshot(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, response)
            return response

        try:
            if self.__indexer.is_snapshot_exist():
                def generate():
                    with self.__indexer.open_snapshot_file() as f:
                        chunk = f.read(1024)
                        yield chunk

                resp = Response(generate(), status=HTTPStatus.OK, mimetype='application/zip', headers={
                    'Content-Disposition': 'attachment; filename=snapshot.zip'
                })
            else:
                resp = Response(status=HTTPStatus.NOT_FOUND)
        except Exception as ex:
            resp = Response(status=HTTPStatus.INTERNAL_SERVER_ERROR)
            self.__logger.error(ex)

        return resp

    def __healthiness(self):
        start_time = time.time()

        @after_this_request
        def to_do_after_this_request(response):
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            healthy = self.__indexer.is_healthy()

            data['healthy'] = healthy
            if healthy:
                status_code = HTTPStatus.OK
            else:
                status_code = HTTPStatus.SERVICE_UNAVAILABLE
                data['error'] = 'node is not healthy'
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
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            alive = self.__indexer.is_alive()

            data['liveness'] = alive
            if alive:
                status_code = HTTPStatus.OK
            else:
                status_code = HTTPStatus.SERVICE_UNAVAILABLE
                data['error'] = 'node is not alive'
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
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, response)
            return response

        data = {}
        status_code = None

        try:
            ready = self.__indexer.is_ready()

            data['readiness'] = ready
            if ready:
                status_code = HTTPStatus.OK
            else:
                status_code = HTTPStatus.SERVICE_UNAVAILABLE
                data['error'] = 'node is not ready'
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
            record_log(request, response, logger=self.__http_logger)
            self.__record_metrics(start_time, request, response)
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
