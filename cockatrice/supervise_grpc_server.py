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

from cockatrice import NAME
from cockatrice.protobuf.index_pb2 import ClearResponse, DeleteResponse, GetResponse, PutResponse
from cockatrice.protobuf.index_pb2_grpc import add_SuperviseServicer_to_server, \
    SuperviseServicer as SuperviseServicerImpl


class SuperviseServicer(SuperviseServicerImpl):
    def __init__(self, supervise_core, logger=getLogger(), metrics_registry=CollectorRegistry()):
        self.__supervise_core = supervise_core
        self.__logger = logger
        self.__metrics_registry = metrics_registry

        # metrics
        self.__metrics_grpc_requests_total = Counter(
            '{0}_supervise_grpc_requests_total'.format(NAME),
            'The number of requests.',
            [
                'func'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_grpc_requests_duration_seconds = Histogram(
            '{0}_supervise_grpc_requests_duration_seconds'.format(NAME),
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

    def Put(self, request, context):
        start_time = time.time()

        response = PutResponse()

        try:
            self.__supervise_core.put(request.key, pickle.loads(request.value), sync=request.sync)

            if request.sync:
                response.status.success = True
                response.status.message = '{0} was successfully created or opened'.format(request.key)
            else:
                response.status.success = True
                response.status.message = 'request was successfully accepted to put {0}'.format(request.key)
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def Get(self, request, context):
        start_time = time.time()

        response = GetResponse()

        try:
            value = self.__supervise_core.get(request.key)
            if value is None:
                response.status.success = False
                response.status.message = '{0} does not exist'.format(request.key)
            else:
                response.value = pickle.dumps(value)

                response.status.success = True
                response.status.message = '{0} was successfully retrieved'.format(request.key)
        except Exception as ex:
            response.status.success = False
            response.status.message = ex.args[0]
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def Delete(self, request, context):
        start_time = time.time()

        response = DeleteResponse()

        try:
            value = self.__supervise_core.delete(request.key, sync=request.sync)

            if request.sync:
                if value is None:
                    if request.key == '/':
                        response.status.success = True
                        response.status.message = '{0} was successfully deleted'.format(request.key)
                    else:
                        response.status.success = False
                        response.status.message = '{0} does not exist'.format(request.key)
                else:
                    response.value = pickle.dumps(value)

                    response.status.success = True
                    response.status.message = '{0} was successfully deleted'.format(request.key)
            else:
                response.status.success = True
                response.status.message = 'request was successfully accepted to delete {0}'.format(request.key)
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response

    def Clear(self, request, context):
        start_time = time.time()

        response = ClearResponse()

        try:
            self.__supervise_core.clear(sync=request.sync)

            if request.sync:
                response.status.success = True
                response.status.message = 'successfully cleared'
            else:
                response.status.success = True
                response.status.message = 'request was successfully accepted to clear'
        except Exception as ex:
            response.status.success = False
            response.status.message = str(ex)
        finally:
            self.__record_grpc_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return response


class SuperviseGRPCServer:
    def __init__(self, supervise_core, host='localhost', port=5050, max_workers=10, logger=getLogger(),
                 metrics_registry=CollectorRegistry()):
        self.supervise_core = supervise_core
        self.__host = host
        self.__port = port
        self.__max_workers = max_workers
        self.__logger = logger
        self.__metrics_registry = metrics_registry

        self.__grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=self.__max_workers))

        add_SuperviseServicer_to_server(
            SuperviseServicer(self.supervise_core, logger=self.__logger, metrics_registry=self.__metrics_registry),
            self.__grpc_server)
        self.__grpc_server.add_insecure_port('{0}:{1}'.format(self.__host, self.__port))

        self.__grpc_server.start()

        self.__logger.info('gRPC server has started')

    def stop(self):
        self.__grpc_server.stop(grace=0.0)

        self.__logger.info('gRPC server has stopped')
