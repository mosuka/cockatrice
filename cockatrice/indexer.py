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

from logging import getLogger
from os import path
from threading import Thread

from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf

from cockatrice.index_core import add_node, get_snapshot, get_status, IndexCore
from cockatrice.index_grpc_server import IndexGRPCServer
from cockatrice.index_http_server import IndexHTTPServer


class Indexer:
    def __init__(self, host='localhost', port=7070, seed_addr=None, conf=SyncObjConf(),
                 index_dir='/tmp/cockatrice/index', grpc_port=5050, grpc_max_workers=10, http_port=8080,
                 logger=getLogger(), http_logger=getLogger(), metrics_registry=CollectorRegistry()):
        self.__host = host
        self.__port = port
        self.__seed_addr = seed_addr
        self.__peer_addrs = []
        self.__conf = conf
        self.__index_dir = index_dir
        self.__grpc_port = grpc_port
        self.__http_port = http_port
        self.__logger = logger
        self.__http_logger = http_logger
        self.__metrics_registry = metrics_registry

        if self.__seed_addr is not None:
            bind_addr = '{0}:{1}'.format(self.__host, self.__port)

            # execute a command to get status from the cluster
            status_result = get_status(bind_addr=self.__seed_addr, timeout=0.5)
            if status_result is not None:
                # get peer addresses from above command result
                self_addr = status_result['self']
                if self_addr not in self.__peer_addrs:
                    self.__peer_addrs.append(self_addr)
                for k in status_result.keys():
                    if k.startswith('partner_node_status_server_'):
                        partner_addr = k[len('partner_node_status_server_'):]
                        if partner_addr not in self.__peer_addrs:
                            self.__peer_addrs.append(partner_addr)

                # add this node to the cluster
                if bind_addr not in self.__peer_addrs:
                    Thread(target=add_node,
                           kwargs={'node_name': bind_addr, 'bind_addr': self.__seed_addr, 'timeout': 0.5}).start()

                # remove this node's address from peer addresses
                if bind_addr in self.__peer_addrs:
                    self.__peer_addrs.remove(bind_addr)

                # get leader node
                leader = status_result['leader']

                # copy snapshot from the leader node
                if not path.exists(self.__conf.fullDumpFile):
                    try:
                        snapshot = get_snapshot(bind_addr=leader, timeout=0.5)
                        if snapshot is not None and len(snapshot) > 0:
                            with open(self.__conf.fullDumpFile, 'wb') as f:
                                f.write(snapshot)
                    except Exception as ex:
                        self.__logger.error('failed to copy snapshot from {0}: {1}'.format(leader, ex))
            else:
                self.__logger.error('failed to get cluster status via {0}'.format(self.__seed_addr))

        self.__index_core = IndexCore(host=self.__host, port=self.__port, peer_addrs=self.__peer_addrs,
                                      conf=self.__conf, index_dir=self.__index_dir, logger=self.__logger,
                                      metrics_registry=self.__metrics_registry)
        self.__index_grpc_server = IndexGRPCServer(self.__index_core, host=self.__host, port=self.__grpc_port,
                                                   max_workers=grpc_max_workers, logger=self.__logger)
        self.__index_http_server = IndexHTTPServer(self.__grpc_port, host=self.__host, port=self.__http_port,
                                                   logger=self.__logger, http_logger=self.__http_logger,
                                                   metrics_registry=self.__metrics_registry)

    def stop(self):
        self.__index_http_server.stop()
        self.__index_core.stop()
