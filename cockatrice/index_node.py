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
    def __init__(self, bind_addr, peer_addrs, conf=None, index_dir='/tmp/cockatrice/index', http_port=8080,
                 logger=getLogger(NAME), http_logger=getLogger(NAME + '_http'), metrics_registry=CollectorRegistry()):
        self.__logger = logger
        self.__index_server = IndexServer(bind_addr, peer_addrs, conf, index_dir, logger)
        self.__index_http_server = IndexHTTPServer(self.__index_server, http_port, logger, http_logger,
                                                   metrics_registry)

    def start(self):
        self.__index_http_server.start()

    def stop(self):
        self.__logger.info('stopping index node')
        self.__index_http_server.stop()
        self.__index_server.destroy()
