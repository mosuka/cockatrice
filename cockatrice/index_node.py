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

from logging import getLogger

from prometheus_client.core import CollectorRegistry

from cockatrice import NAME
from cockatrice.index_server import IndexServer
from cockatrice.index_http_server import IndexHTTPServer


class IndexNode:
    def __init__(self, bind_addr, peer_addrs=None, conf=None, index_dir=None, http_port=8080,
                 logger=getLogger(NAME), http_logger=getLogger(NAME + '_http'), metrics_registry=CollectorRegistry()):
        self.__bind_addr = bind_addr
        self.__peer_addrs = peer_addrs
        self.__conf = conf
        self.__index_dir = index_dir
        self.__http_port = http_port
        self.__logger = logger
        self.__http_logger = http_logger
        self.__metrics_registry = metrics_registry

        self.__index_server = None
        self.__index_http_server = None

    def start(self):
        self.__logger.info('starting index node')

        self.__index_server = IndexServer(self.__bind_addr, self.__peer_addrs, conf=self.__conf,
                                          index_dir=self.__index_dir, logger=self.__logger)
        self.__index_http_server = IndexHTTPServer(self.__index_server, port=self.__http_port, logger=self.__logger,
                                                   http_logger=self.__http_logger,
                                                   metrics_registry=self.__metrics_registry)

        self.__index_http_server.start()

    def stop(self):
        self.__logger.info('stopping index node')
        self.__index_http_server.stop()
        self.__index_server.destroy()
