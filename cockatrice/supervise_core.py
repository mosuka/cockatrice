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
import socket
import threading
import time
import zipfile
from contextlib import closing
from logging import getLogger

import pysyncobj.pickle as pickle
from locked_dict.locked_dict import LockedDict
from prometheus_client.core import CollectorRegistry
from pysyncobj import replicated, SyncObj, SyncObjConf

from cockatrice.util.raft import RAFT_DATA_FILE
from cockatrice.util.resolver import parse_addr


class SuperviseCore(SyncObj):
    def __init__(self, host='localhost', port=7070, peer_addrs=None, conf=SyncObjConf(),
                 data_dir='/tmp/cockatrice/supervise', logger=getLogger(), metrics_registry=CollectorRegistry()):
        self.__logger = logger
        self.__metrics_registry = metrics_registry

        self.__lock = threading.RLock()

        self.__bind_addr = '{0}:{1}'.format(host, port)
        self.__peer_addrs = [] if peer_addrs is None else peer_addrs
        self.__data_dir = data_dir
        self.__conf = conf
        self.__conf.serializer = self.__serialize
        self.__conf.deserializer = self.__deserialize
        self.__conf.validate()

        self.__data = LockedDict()

        os.makedirs(self.__data_dir, exist_ok=True)

        super(SuperviseCore, self).__init__(self.__bind_addr, self.__peer_addrs, conf=self.__conf)
        self.__logger.info('supervise core has started')

        # waiting for the preparation to be completed
        while not self.isReady():
            # recovering data
            self.__logger.debug('waiting for the cluster ready')
            time.sleep(1)
        self.__logger.info('supervise core ready')

    def stop(self):
        self.destroy()
        self.__logger.info('supervise core has stopped')

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
        return self.is_alive() and self.is_ready()

    def is_alive(self):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            alive = sock.connect_ex(parse_addr(self.__bind_addr)) == 0
        return alive

    def is_ready(self):
        return self.isReady()

    def __key_value_to_dict(self, key, value):
        keys = [k for k in key.split('/') if k != '']

        if len(keys) > 1:
            value = self.__key_value_to_dict('/'.join(keys[1:]), value)

        return {keys[0]: value}

    def __put(self, key, value):
        if key == '/':
            self.__data.update(value)
        else:
            self.__data.update(self.__key_value_to_dict(key, value))

    @replicated
    def put(self, key, value):
        self.__put(key, value)

    def get(self, key):
        value = self.__data
        keys = [k for k in key.split('/') if k != '']

        for k in keys:
            value = value.get(k, None)
            if value is None:
                return None

        return value

    def __delete(self, key):
        if key == '/':
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

    @replicated
    def delete(self, key):
        return self.__delete(key)

    def __clear(self):
        self.__data.clear()

    @replicated
    def clear(self):
        self.__clear()
