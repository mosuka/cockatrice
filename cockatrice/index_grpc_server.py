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
import inspect
import time
from concurrent import futures
from logging import getLogger

import grpc
from prometheus_client.core import CollectorRegistry, Counter, Histogram
from whoosh.scoring import BM25F

from cockatrice import NAME
from cockatrice.protobuf.index_pb2 import CloseIndexResponse, CreateIndexResponse, CreateSnapshotResponse, \
    DeleteDocumentResponse, DeleteDocumentsResponse, DeleteIndexResponse, DeleteNodeResponse, GetDocumentResponse, \
    GetIndexResponse, GetSnapshotResponse, GetStatusResponse, IsAliveResponse, IsHealthyResponse, IsReadyResponse, \
    OpenIndexResponse, OptimizeIndexResponse, PutDocumentResponse, PutDocumentsResponse, PutNodeResponse, \
    SearchDocumentsResponse, SnapshotExistsResponse, Status
from cockatrice.protobuf.index_pb2_grpc import add_IndexServicer_to_server, IndexServicer as IndexServicerImpl
from cockatrice.schema import Schema
from cockatrice.scoring import get_multi_weighting


class IndexServicer(IndexServicerImpl):
    def __init__(self, index_core, logger=getLogger(), metrics_registry=CollectorRegistry()):
        self.__index_core = index_core
        self.__logger = logger
        self.__metrics_registry = metrics_registry

        # metrics
        self.__metrics_grpc_requests_total = Counter(
            '{0}_index_grpc_requests_total'.format(NAME),
            'The number of requests.',
            [
                'func'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_grpc_requests_duration_seconds = Histogram(
            '{0}_index_grpc_requests_duration_seconds'.format(NAME),
            'The invocation duration in seconds.',
            [
                'func'
            ],
            registry=self.__metrics_registry
        )

    def __record_grpc_metrics(self, start_time, func_name):
        self.__metrics_grpc_requests_total.labels(
            func=func_name
        ).inc()

        self.__metrics_grpc_requests_duration_seconds.labels(
            func=func_name
        ).observe(time.time() - start_time)

        return

    def CreateIndex(self, request, context):
        start_time = time.time()

        response = CreateIndexResponse()

        try:
            schema = Schema(pickle.loads(request.schema))
            index = self.__index_core.create_index(request.index_name, schema, sync=request.sync)

            if request.sync:
                if index is None:
                    response.status.success = False
                    response.status.message = 'failed to create {0}'.format(request.index_name)
                else:
                    response.index_stats.name = index.indexname
                    response.index_stats.doc_count = index.doc_count()
                    response.index_stats.doc_count_all = index.doc_count_all()
                    response.index_stats.latest_generation = index.latest_generation()
                    response.index_stats.version = index.version
                    response.index_stats.storage.folder = index.storage.folder
                    response.index_stats.storage.supports_mmap = index.storage.supports_mmap
                    response.index_stats.storage.readonly = index.storage.readonly
                    response.index_stats.storage.files.extend(index.storage.list())

                    response.status.success = True
                    response.status.message = '{0} was successfully created or opened'.format(index.indexname)
            else:
                response.status.success = True
                response.status.message = 'request was successfully accepted to create {0}'.format(request.index_name)
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def GetIndex(self, request, context):
        start_time = time.time()

        response = GetIndexResponse()

        try:
            index = self.__index_core.get_index(request.index_name)

            response.index_stats.name = index.indexname
            response.index_stats.doc_count = index.doc_count()
            response.index_stats.doc_count_all = index.doc_count_all()
            response.index_stats.latest_generation = index.latest_generation()
            response.index_stats.version = index.version
            response.index_stats.storage.folder = index.storage.folder
            response.index_stats.storage.supports_mmap = index.storage.supports_mmap
            response.index_stats.storage.readonly = index.storage.readonly
            response.index_stats.storage.files.extend(index.storage.list())

            response.status.success = True
            response.status.message = '{0} was successfully retrieved'.format(index.indexname)
        except Exception as ex:
            response.status.success = False
            response.status.message = ex.args[0]
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def DeleteIndex(self, request, context):
        start_time = time.time()

        response = DeleteIndexResponse()

        try:
            index = self.__index_core.delete_index(request.index_name, sync=request.sync)

            if request.sync:
                if index is None:
                    response.status.success = False
                    response.status.message = 'failed to delete {0}'.format(request.index_name)
                else:
                    response.index_stats.name = index.indexname
                    # response.index_stats.doc_count = index.doc_count()
                    # response.index_stats.doc_count_all = index.doc_count_all()
                    response.index_stats.latest_generation = index.latest_generation()
                    # response.index_stats.version = index.version
                    response.index_stats.storage.folder = index.storage.folder
                    response.index_stats.storage.supports_mmap = index.storage.supports_mmap
                    response.index_stats.storage.readonly = index.storage.readonly
                    response.index_stats.storage.files.extend(index.storage.list())

                    response.status.success = True
                    response.status.message = '{0} was successfully deleted'.format(index.indexname)
            else:
                response.status.success = True
                response.status.message = 'request was successfully accepted to close {0}'.format(request.index_name)
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def OpenIndex(self, request, context):
        start_time = time.time()

        response = OpenIndexResponse()

        try:
            schema = None if request.schema == b'' else Schema(pickle.loads(request.schema))
            index = self.__index_core.open_index(request.index_name, schema=schema, sync=request.sync)

            if request.sync:
                if index is None:
                    response.status.success = False
                    response.status.message = 'failed to open {0}'.format(request.index_name)
                else:
                    response.index_stats.name = index.indexname
                    response.index_stats.doc_count = index.doc_count()
                    response.index_stats.doc_count_all = index.doc_count_all()
                    response.index_stats.latest_generation = index.latest_generation()
                    response.index_stats.version = index.version
                    response.index_stats.storage.folder = index.storage.folder
                    response.index_stats.storage.supports_mmap = index.storage.supports_mmap
                    response.index_stats.storage.readonly = index.storage.readonly
                    response.index_stats.storage.files.extend(index.storage.list())

                    response.status.success = True
                    response.status.message = '{0} was successfully opened'.format(index.indexname)
            else:
                response.status.success = True
                response.status.message = 'request was successfully accepted to open {0}'.format(request.index_name)
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def CloseIndex(self, request, context):
        start_time = time.time()

        response = CloseIndexResponse()

        try:
            index = self.__index_core.close_index(request.index_name, sync=request.sync)

            if request.sync:
                if index is None:
                    response.status.success = False
                    response.status.message = 'failed to close {0}'.format(request.index_name)
                else:
                    response.index_stats.name = index.indexname
                    response.index_stats.doc_count = index.doc_count()
                    response.index_stats.doc_count_all = index.doc_count_all()
                    response.index_stats.latest_generation = index.latest_generation()
                    response.index_stats.version = index.version
                    response.index_stats.storage.folder = index.storage.folder
                    response.index_stats.storage.supports_mmap = index.storage.supports_mmap
                    response.index_stats.storage.readonly = index.storage.readonly
                    response.index_stats.storage.files.extend(index.storage.list())

                    response.status.success = True
                    response.status.message = '{0} was successfully closed'.format(index.indexname)
            else:
                response.status.success = True
                response.status.message = 'request was successfully accepted to close {0}'.format(request.index_name)
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def OptimizeIndex(self, request, context):
        start_time = time.time()

        response = OptimizeIndexResponse()

        try:
            index = self.__index_core.optimize_index(request.index_name, sync=request.sync)

            if request.sync:
                if index is None:
                    response.status.success = False
                    response.status.message = 'failed to optimize {0}'.format(request.index_name)
                else:
                    response.index_stats.name = index.indexname
                    response.index_stats.doc_count = index.doc_count()
                    response.index_stats.doc_count_all = index.doc_count_all()
                    response.index_stats.latest_generation = index.latest_generation()
                    response.index_stats.version = index.version
                    response.index_stats.storage.folder = index.storage.folder
                    response.index_stats.storage.supports_mmap = index.storage.supports_mmap
                    response.index_stats.storage.readonly = index.storage.readonly
                    response.index_stats.storage.files.extend(index.storage.list())

                    response.status.success = True
                    response.status.message = '{0} was successfully optimized'.format(index.indexname)
            else:
                response.status.success = True
                response.status.message = 'request was successfully accepted to optimize {0}'.format(request.index_name)
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def PutDocument(self, request, context):
        start_time = time.time()

        response = PutDocumentResponse()

        try:
            count = self.__index_core.put_document(request.index_name, request.doc_id, pickle.loads(request.fields),
                                                   sync=request.sync)
            if request.sync:
                response.count = count
                if response.count > 0:
                    response.status.success = True
                    response.status.message = '{0} was successfully put to {1}'.format(request.doc_id,
                                                                                       request.index_name)
                else:
                    response.status.success = False
                    response.status.message = 'failed to put {0} to {1}'.format(request.document.id, request.index_name)
            else:
                response.status.success = True
                response.status.message = 'request was successfully accepted to put {0} to {1}'.format(request.doc_id,
                                                                                                       request.index_name)
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def GetDocument(self, request, context):
        start_time = time.time()

        response = GetDocumentResponse()

        try:
            results_page = self.__index_core.get_document(request.index_name, request.doc_id)

            if results_page.total > 0:
                fields = {}
                for i in results_page.results[0].iteritems():
                    fields[i[0]] = i[1]
                response.fields = pickle.dumps(fields)

                response.status.success = True
                response.status.message = '{0} was successfully got from {1}'.format(request.doc_id, request.index_name)
            else:
                response.status.success = False
                response.status.message = '{0} does not exist in {1}'.format(request.doc_id, request.index_name)
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def DeleteDocument(self, request, context):
        start_time = time.time()

        response = DeleteDocumentResponse()

        try:
            count = self.__index_core.delete_document(request.index_name, request.doc_id, sync=request.sync)

            if request.sync:
                response.count = count
                if response.count > 0:
                    response.status.success = True
                    response.status.message = '{0} was successfully deleted from {1}'.format(request.doc_id,
                                                                                             request.index_name)
                elif response.count == 0:
                    response.status.success = False
                    response.status.message = '{0} does not exist in {1}'.format(request.doc_id, request.index_name)
                else:
                    response.status.success = False
                    response.status.message = 'failed to delete {0} to {1}'.format(request.document.id,
                                                                                   request.index_name)
            else:
                response.status.success = True
                response.status.message = 'request was successfully accepted to delete {0} to {1}'.format(
                    request.doc_id, request.index_name)
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def PutDocuments(self, request, context):
        start_time = time.time()

        response = PutDocumentsResponse()

        try:
            count = self.__index_core.put_documents(request.index_name, pickle.loads(request.docs),
                                                    sync=request.sync)
            if request.sync:
                response.count = count
                if response.count > 0:
                    response.status.success = True
                    response.status.message = '{0} documents were successfully put to {1}'.format(response.count,
                                                                                                  request.index_name)
                else:
                    response.status.success = False
                    response.status.message = 'failed to put documents to {0}'.format(request.index_name)
            else:
                response.status.success = True
                response.status.message = 'request was successfully accepted to put documents to {0}'.format(
                    request.index_name)
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def DeleteDocuments(self, request, context):
        start_time = time.time()

        response = DeleteDocumentsResponse()

        try:
            count = self.__index_core.delete_documents(request.index_name, pickle.loads(request.doc_ids),
                                                       sync=request.sync)
            if request.sync:
                response.count = count
                if response.count > 0:
                    response.status.success = True
                    response.status.message = '{0} documents were successfully deleted from {1}'.format(response.count,
                                                                                                        request.index_name)
                else:
                    response.status.success = False
                    response.status.message = 'failed to delete documents from {0}'.format(request.index_name)
            else:
                response.status.success = True
                response.status.message = 'request was successfully accepted to delete documents to {0}'.format(
                    request.index_name)
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def SearchDocuments(self, request, context):
        start_time = time.time()

        response = SearchDocumentsResponse()

        try:
            search_field = request.search_field if request.search_field != '' else self.__index_core.get_schema(
                request.index_name).get_default_search_field()
            weighting = BM25F if request.weighting == b'' else get_multi_weighting(pickle.loads(request.weighting))

            results_page = self.__index_core.search_documents(request.index_name, request.query, search_field,
                                                              request.page_num, page_len=request.page_len,
                                                              weighting=weighting)

            if results_page.pagecount >= request.page_num or results_page.total <= 0:
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

                response.results = pickle.dumps(results)

                response.status.success = True
                response.status.message = '{0} documents were successfully searched from {1}'.format(results_page.total,
                                                                                                     request.index_name)
            else:
                response.status.success = False
                response.status.message = 'page_num must be <= {0}'.format(results_page.pagecount)
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def PutNode(self, request, context):
        start_time = time.time()

        response = PutNodeResponse()

        try:
            self.__index_core.addNodeToCluster(request.node_name)

            response.status.success = True
            response.status.message = '{0} was successfully added to the cluster'.format(request.node_name)
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def DeleteNode(self, request, context):
        start_time = time.time()

        response = DeleteNodeResponse()

        try:
            self.__index_core.removeNodeFromCluster(request.node_name)

            response.status.success = True
            response.status.message = '{0} was successfully deleted from the cluster'.format(request.node_name)
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def SnapshotExists(self, request, context):
        start_time = time.time()

        response = SnapshotExistsResponse()

        try:
            response.exist = self.__index_core.snapshot_exists()

            response.status.success = True
            response.status.message = 'snapshot exists' if response.exist else 'snapshot does not exist'
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def CreateSnapshot(self, request, context):
        start_time = time.time()

        response = CreateSnapshotResponse()

        try:
            self.__index_core.forceLogCompaction()

            response.status.success = True
            response.status.message = 'request was successfully accepted'
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def GetSnapshot(self, request, context):
        start_time = time.time()

        def get_snapshot_chunks(chunk_size=1024):
            with self.__index_core.open_snapshot_file() as f:
                while True:
                    chunk = f.read(chunk_size)
                    if len(chunk) == 0:
                        return
                    status = Status()
                    status.success = True
                    status.message = 'successfully got snapshot chunk'
                    yield GetSnapshotResponse(name=self.__index_core.get_snapshot_file_name(), chunk=chunk,
                                              status=status)

        try:
            response = get_snapshot_chunks(chunk_size=request.chunk_size)
        except Exception as ex:
            response = GetSnapshotResponse()
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def IsHealthy(self, request, context):
        start_time = time.time()

        response = IsHealthyResponse()

        try:
            response.healthy = self.__index_core.is_healthy()

            response.status.success = True
            response.status.message = 'node is alive' if response.healthy else 'node is dead'
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def IsAlive(self, request, context):
        start_time = time.time()

        response = IsAliveResponse()

        try:
            response.alive = self.__index_core.is_alive()

            response.status.success = True
            response.status.message = 'node is alive' if response.alive else 'node is dead'
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def IsReady(self, request, context):
        start_time = time.time()

        response = IsReadyResponse()

        try:
            response.ready = self.__index_core.is_ready()

            response.status.success = True
            response.status.message = 'cluster is ready' if response.ready else 'cluster not ready'
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def GetStatus(self, request, context):
        start_time = time.time()

        response = GetStatusResponse()

        try:
            response.node_status = pickle.dumps(self.__index_core.getStatus())

            response.status.success = True
            response.status.message = 'successfully got cluster status'
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response


class IndexGRPCServer:
    def __init__(self, index_server, host='localhost', port=5050, max_workers=10, logger=getLogger(),
                 metrics_registry=CollectorRegistry()):
        self.__index_server = index_server
        self.__host = host
        self.__port = port
        self.__max_workers = max_workers
        self.__logger = logger
        self.__metrics_registry = metrics_registry

        self.__grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=self.__max_workers))

        add_IndexServicer_to_server(
            IndexServicer(self.__index_server, logger=self.__logger, metrics_registry=self.__metrics_registry),
            self.__grpc_server)
        self.__grpc_server.add_insecure_port('{0}:{1}'.format(self.__host, self.__port))

        self.__grpc_server.start()

        self.__logger.info('gRPC server started')

    def stop(self):
        self.__grpc_server.stop(grace=0.0)
