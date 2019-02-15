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

import os
import time
import zipfile
from concurrent import futures
from logging import getLogger
from threading import RLock, Thread

import grpc
import pysyncobj.pickle as pickle
from locked_dict.locked_dict import LockedDict
from prometheus_client.core import CollectorRegistry, Counter, Histogram
from pysyncobj import replicated, SyncObjConf

from cockatrice import NAME
from cockatrice.manager_grpc import ManagementGRPCServicer
from cockatrice.manager_http import ManagementHTTPServicer
from cockatrice.protobuf.management_pb2_grpc import add_ManagementServicer_to_server
from cockatrice.util.http import HTTPServer
from cockatrice.util.raft import add_node, get_peers, get_snapshot, get_status, RAFT_DATA_FILE, RaftNode


class Manager(RaftNode):
    def __init__(self, host='localhost', port=7070, seed_addr=None, conf=SyncObjConf(),
                 data_dir='/tmp/cockatrice/management', grpc_port=5050, grpc_max_workers=10, http_port=8080,
                 logger=getLogger(), http_logger=getLogger(), metrics_registry=CollectorRegistry()):

        self.__host = host
        self.__port = port
        self.__seed_addr = seed_addr
        self.__conf = conf
        self.__data_dir = data_dir
        self.__grpc_port = grpc_port
        self.__grpc_max_workers = grpc_max_workers
        self.__http_port = http_port
        self.__logger = logger
        self.__http_logger = http_logger
        self.__metrics_registry = metrics_registry

        # metrics
        self.__metrics_requests_total = Counter(
            '{0}_manager_requests_total'.format(NAME),
            'The number of requests.',
            [
                'func'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_requests_duration_seconds = Histogram(
            '{0}_manager_requests_duration_seconds'.format(NAME),
            'The invocation duration in seconds.',
            [
                'func'
            ],
            registry=self.__metrics_registry
        )

        self.__self_addr = '{0}:{1}'.format(self.__host, self.__port)
        self.__peer_addrs = [] if self.__seed_addr is None else get_peers(self.__seed_addr)
        self.__other_addrs = [peer_addr for peer_addr in self.__peer_addrs if peer_addr != self.__self_addr]
        self.__conf.serializer = self.__serialize
        self.__conf.deserializer = self.__deserialize
        self.__conf.validate()

        self.__data = LockedDict()
        self.__lock = RLock()

        # add this node to the cluster
        if self.__self_addr not in self.__peer_addrs and self.__seed_addr is not None:
            Thread(target=add_node,
                   kwargs={'node_name': self.__self_addr, 'bind_addr': self.__seed_addr, 'timeout': 0.5}).start()

        # create data dir
        os.makedirs(self.__data_dir, exist_ok=True)

        # copy snapshot from the leader node
        if self.__seed_addr is not None:
            try:
                leader = get_status(bind_addr=self.__seed_addr, timeout=0.5)['leader']
                self.__logger.info('copying snapshot from {0}'.format(leader))
                snapshot = get_snapshot(bind_addr=leader, timeout=0.5)
                if snapshot is not None:
                    with open(self.__conf.fullDumpFile, 'wb') as f:
                        f.write(snapshot)
                    self.__logger.info('snapshot copied from {0}'.format(leader))
            except Exception as ex:
                self.__logger.error('failed to copy snapshot from {0}: {1}'.format(leader, ex))

        # start node
        super(Manager, self).__init__(self.__self_addr, self.__other_addrs, conf=self.__conf)
        self.__logger.info('state machine has started')
        while not self.isReady():
            # recovering data
            self.__logger.debug('waiting for cluster ready')
            time.sleep(1)
        self.__logger.info('cluster ready')

        # start gRPC
        self.__grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=self.__grpc_max_workers))
        add_ManagementServicer_to_server(
            ManagementGRPCServicer(self, logger=self.__logger, metrics_registry=self.__metrics_registry),
            self.__grpc_server)
        self.__grpc_server.add_insecure_port('{0}:{1}'.format(self.__host, self.__grpc_port))
        self.__grpc_server.start()
        self.__logger.info('gRPC server has started')

        # start HTTP server
        self.__http_servicer = ManagementHTTPServicer(self, self.__logger, self.__http_logger, self.__metrics_registry)
        self.__http_server = HTTPServer(self.__host, self.__http_port, self.__http_servicer)
        self.__http_server.start()
        self.__logger.info('HTTP server has started')

        self.__logger.info('manager has started')

    def stop(self):
        # stop HTTP server
        self.__http_server.stop()
        self.__logger.info('HTTP server has stopped')

        # stop gRPC server
        self.__grpc_server.stop(grace=0.0)
        self.__logger.info('gRPC server has stopped')

        # stop node
        self.destroy()
        self.__logger.info('state machine has stopped')

        self.__logger.info('manager has stopped')

    def __record_metrics(self, start_time, func_name):
        self.__metrics_requests_total.labels(
            func=func_name
        ).inc()

        self.__metrics_requests_duration_seconds.labels(
            func=func_name
        ).observe(time.time() - start_time)

    # serializer
    def __serialize(self, filename, raft_data):
        with self.__lock:
            try:
                self.__logger.info('serializer has started')

                with zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as f:
                    # store the federation data
                    f.writestr('federation.bin', pickle.dumps(self.__data))
                    self.__logger.debug('federation data has stored in {0}'.format(filename))

                    # store the raft data
                    f.writestr(RAFT_DATA_FILE, pickle.dumps(raft_data))
                    self.__logger.info('{0} has restored'.format(RAFT_DATA_FILE))
                self.__logger.info('snapshot has created')
            except Exception as ex:
                self.__logger.error('failed to create snapshot: {0}'.format(ex))
            finally:
                self.__logger.info('serializer has stopped')

    # deserializer
    def __deserialize(self, filename):
        raft_data = None

        with self.__lock:
            try:
                self.__logger.info('deserializer has started')

                with zipfile.ZipFile(filename, 'r') as zf:
                    # extract the federation data
                    zf.extract('federation.bin', path=self.__data_dir)
                    self.__data = pickle.loads(zf.read('federation.bin'))
                    self.__logger.info('federation.bin has restored')

                    # restore the raft data
                    raft_data = pickle.loads(zf.read(RAFT_DATA_FILE))
                    self.__logger.info('raft.{0} has restored'.format(RAFT_DATA_FILE))
                self.__logger.info('snapshot has restored')
            except Exception as ex:
                self.__logger.error('failed to restore indices: {0}'.format(ex))
            finally:
                self.__logger.info('deserializer has stopped')

        return raft_data

    def is_healthy(self):
        return self.isHealthy()

    def is_alive(self):
        return self.isAlive()

    def is_ready(self):
        return self.isReady()

    def __key_value_to_dict(self, key, value):
        keys = [k for k in key.split('/') if k != '']

        if len(keys) > 1:
            value = self.__key_value_to_dict('/'.join(keys[1:]), value)

        return {keys[0]: value}

    def __put(self, key, value):
        start_time = time.time()

        try:
            if key == '/':
                self.__data.update(value)
            else:
                self.__data.update(self.__key_value_to_dict(key, value))
        finally:
            self.__record_metrics(start_time, 'put')

    @replicated
    def put(self, key, value):
        self.__put(key, value)

    def get(self, key):
        start_time = time.time()

        try:
            value = self.__data
            keys = [k for k in key.split('/') if k != '']

            for k in keys:
                value = value.get(k, None)
                if value is None:
                    return None
        finally:
            self.__record_metrics(start_time, 'get')

        return value

    def __delete(self, key):
        start_time = time.time()

        try:
            if key == '/':
                value = dict(self.__data)
                self.__clear()
            else:
                keys = [k for k in key.split('/') if k != '']
                value = self.__data

                i = 0
                while i < len(keys):
                    if len(keys[i:]) == 1:
                        return value.pop(keys[i], None)

                    value = value.get(keys[i], None)
                    if value is None:
                        return None

                    i += 1
        finally:
            self.__record_metrics(start_time, 'delete')

        return value

    @replicated
    def delete(self, key):
        return self.__delete(key)

    def __clear(self):
        start_time = time.time()

        try:
            self.__data.clear()
        finally:
            self.__record_metrics(start_time, 'clear')

    @replicated
    def clear(self):
        self.__clear()
