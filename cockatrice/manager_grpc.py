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
import time
from logging import getLogger

from prometheus_client.core import CollectorRegistry, Counter, Histogram

from cockatrice import NAME
from cockatrice.protobuf.management_pb2 import ClearResponse, DeleteResponse, GetResponse, PutResponse
from cockatrice.protobuf.management_pb2_grpc import ManagementServicer


class ManagementGRPCServicer(ManagementServicer):
    def __init__(self, manager, logger=getLogger(), metrics_registry=CollectorRegistry()):
        self.__manager = manager
        self.__logger = logger
        self.__metrics_registry = metrics_registry

        # metrics
        self.__metrics_requests_total = Counter(
            '{0}_manager_grpc_requests_total'.format(NAME),
            'The number of requests.',
            [
                'func'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_requests_duration_seconds = Histogram(
            '{0}_manager_grpc_requests_duration_seconds'.format(NAME),
            'The invocation duration in seconds.',
            [
                'func'
            ],
            registry=self.__metrics_registry
        )

    def __record_metrics(self, start_time, func_name):
        self.__metrics_requests_total.labels(
            func=func_name
        ).inc()

        self.__metrics_requests_duration_seconds.labels(
            func=func_name
        ).observe(time.time() - start_time)

        return

    def Put(self, request, context):
        start_time = time.time()

        response = PutResponse()

        try:
            self.__manager.put(request.key, pickle.loads(request.value), sync=request.sync)

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
            self.__record_metrics(start_time, 'put')

        return response

    def Get(self, request, context):
        start_time = time.time()

        response = GetResponse()

        try:
            value = self.__manager.get(request.key)
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
            self.__record_metrics(start_time, 'get')

        return response

    def Delete(self, request, context):
        start_time = time.time()

        response = DeleteResponse()

        try:
            value = self.__manager.delete(request.key, sync=request.sync)

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
            self.__record_metrics(start_time, 'get')

        return response

    def Clear(self, request, context):
        start_time = time.time()

        response = ClearResponse()

        try:
            self.__manager.clear(sync=request.sync)

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
            self.__record_metrics(start_time, 'clear')

        return response
