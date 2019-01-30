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

import copy
import functools
import inspect
import os
import re
import socket
import threading
import time
import zipfile
from contextlib import closing
from logging import getLogger

import pysyncobj.pickle as pickle
from prometheus_client.core import CollectorRegistry, Counter, Gauge, Histogram
from pysyncobj import FAIL_REASON, replicated, SyncObj, SyncObjConf
from pysyncobj.encryptor import getEncryptor
from pysyncobj.poller import createPoller
from pysyncobj.tcp_connection import TcpConnection
from whoosh.filedb.filestore import FileStorage, RamStorage
from whoosh.qparser import QueryParser

from cockatrice import NAME
from cockatrice.index_writer import IndexWriter
from cockatrice.util.resolver import get_ipv4, parse_addr
from cockatrice.util.timer import RepeatedTimer


class IndexCoreCommand:
    def __init__(self, cmd, args=None, bind_addr='localhost:7070', password=None, timeout=1):
        try:
            self.__result = None

            self.__request = [cmd]
            if args is not None:
                self.__request.extend(args)

            host, port = parse_addr(bind_addr)

            self.__host = get_ipv4(host)
            self.__port = int(port)
            self.__password = password
            self.__poller = createPoller('auto')
            self.__connection = TcpConnection(self.__poller, onMessageReceived=self.__on_message_received,
                                              onConnected=self.__on_connected, onDisconnected=self.__on_disconnected,
                                              socket=None, timeout=10.0, sendBufferSize=2 ** 13, recvBufferSize=2 ** 13)
            if self.__password is not None:
                self.__connection.encryptor = getEncryptor(self.__password)
            self.__is_connected = self.__connection.connect(self.__host, self.__port)
            while self.__is_connected:
                self.__poller.poll(timeout)
        except Exception as ex:
            raise ex

    def __on_message_received(self, message):
        if self.__connection.encryptor and not self.__connection.sendRandKey:
            self.__connection.sendRandKey = message
            self.__connection.send(self.__request)
            return

        self.__result = message
        self.__connection.disconnect()

    def __on_connected(self):
        if self.__connection.encryptor:
            self.__connection.recvRandKey = os.urandom(32)
            self.__connection.send(self.__connection.recvRandKey)
            return

        self.__connection.send(self.__request)

    def __on_disconnected(self):
        self.__is_connected = False

    def get_result(self):
        return self.__result


def execute(cmd, args=None, bind_addr='127.0.0.1:7070', password=None, timeout=1):
    try:
        response = IndexCoreCommand(cmd, args=args, bind_addr=bind_addr, password=password,
                                    timeout=timeout).get_result()
    except Exception as ex:
        raise ex

    return response


def get_status(bind_addr='127.0.0.1:7070', password=None, timeout=1):
    return execute('status', args=None, bind_addr=bind_addr, password=password, timeout=timeout)


def add_node(node_name, bind_addr='127.0.0.1:7070', password=None, timeout=1):
    return execute('add', args=[node_name], bind_addr=bind_addr, password=password, timeout=timeout)


def delete_node(node_name, bind_addr='127.0.0.1:7070', password=None, timeout=1):
    return execute('remove', args=[node_name], bind_addr=bind_addr, password=password, timeout=timeout)


def get_snapshot(bind_addr='127.0.0.1:7070', password=None, timeout=1):
    return execute('get_snapshot', args=None, bind_addr=bind_addr, password=password, timeout=timeout)


def is_alive(bind_addr='127.0.0.1:7070', password=None, timeout=1):
    return execute('is_alive', args=None, bind_addr=bind_addr, password=password, timeout=timeout) == 'True'


def is_ready(bind_addr='127.0.0.1:7070', password=None, timeout=1):
    return execute('is_ready', args=None, bind_addr=bind_addr, password=password, timeout=timeout) == 'True'


class IndexCore(SyncObj):
    def __init__(self, host='localhost', port=7070, peer_addrs=None, conf=SyncObjConf(),
                 index_dir='/tmp/cockatrice/index', logger=getLogger(), metrics_registry=CollectorRegistry()):
        self.__logger = logger
        self.__metrics_registry = metrics_registry

        # metrics
        self.__metrics_core_documents = Gauge(
            '{0}_index_core_documents'.format(NAME),
            'The number of documents.',
            [
                'index_name',
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_core_requests_total = Counter(
            '{0}_index_core_requests_total'.format(NAME),
            'The number of requests.',
            [
                'func'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_core_requests_duration_seconds = Histogram(
            '{0}_index_core_requests_duration_seconds'.format(NAME),
            'The invocation duration in seconds.',
            [
                'func'
            ],
            registry=self.__metrics_registry
        )

        self.__lock = threading.RLock()

        self.__bind_addr = '{0}:{1}'.format(host, port)
        self.__peer_addrs = [] if peer_addrs is None else peer_addrs
        self.__index_dir = index_dir
        self.__conf = conf
        self.__conf.serializer = self.__serialize
        self.__conf.deserializer = self.__deserialize
        self.__conf.validate()

        self.__indices = {}
        self.__index_configs = {}
        self.__writers = {}

        os.makedirs(self.__index_dir, exist_ok=True)
        self.__file_storage = FileStorage(self.__index_dir, supports_mmap=True, readonly=False, debug=False)
        self.__ram_storage = RamStorage()

        super(IndexCore, self).__init__(self.__bind_addr, self.__peer_addrs, conf=self.__conf)
        self.__logger.info('index core has started')

        # waiting for the preparation to be completed
        while not self.isReady():
            # recovering data
            self.__logger.debug('waiting for the cluster ready')
            time.sleep(1)
        self.__logger.info('index core ready')

        # open existing indices on startup
        for index_name in self.get_index_names():
            self.__open_index(index_name, index_config=None)

        self.metrics_timer = RepeatedTimer(10, self.__record_metrics)
        self.metrics_timer.start()

    def stop(self):
        self.metrics_timer.cancel()

        # close indices
        for index_name in list(self.__indices.keys()):
            self.__close_index(index_name)

        self.destroy()

        self.__logger.info('index core has stopped')

    def destroy(self):
        super().destroy()

    def __record_metrics(self):
        for index_name in list(self.__indices.keys()):
            try:
                self.__metrics_core_documents.labels(index_name=index_name).set(self.get_doc_count(index_name))
            except Exception as ex:
                self.__logger.error(ex)

    def __record_core_metrics(self, start_time, func_name):
        self.__metrics_core_requests_total.labels(
            func=func_name
        ).inc()

        self.__metrics_core_requests_duration_seconds.labels(
            func=func_name
        ).observe(time.time() - start_time)

    def is_healthy(self):
        return self.is_alive() and self.is_ready()

    def is_alive(self):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            alive = sock.connect_ex(parse_addr(self.__bind_addr)) == 0

        return alive

    def is_ready(self):
        return self.isReady()

    def get_addr(self):
        return self.__bind_addr

    # override SyncObj.__utilityCallback
    def _SyncObj__utilityCallback(self, res, err, conn, cmd, node):
        cmdResult = 'FAIL'
        if err == FAIL_REASON.SUCCESS:
            cmdResult = 'SUCCESS'
        conn.send(cmdResult + ' ' + cmd + ' ' + node)

    # override SyncObj.__onUtilityMessage
    def _SyncObj__onUtilityMessage(self, conn, message):
        try:
            if message[0] == 'status':
                conn.send(self.getStatus())
                return True
            elif message[0] == 'add':
                self.addNodeToCluster(message[1],
                                      callback=functools.partial(self._SyncObj__utilityCallback, conn=conn, cmd='ADD',
                                                                 node=message[1]))
                return True
            elif message[0] == 'remove':
                if message[1] == self.__selfNodeAddr:
                    conn.send('FAIL REMOVE ' + message[1])
                else:
                    self.removeNodeFromCluster(message[1],
                                               callback=functools.partial(self._SyncObj__utilityCallback, conn=conn,
                                                                          cmd='REMOVE', node=message[1]))
                return True
            elif message[0] == 'set_version':
                self.setCodeVersion(message[1],
                                    callback=functools.partial(self._SyncObj__utilityCallback, conn=conn,
                                                               cmd='SET_VERSION', node=str(message[1])))
                return True
            elif message[0] == 'get_snapshot':
                if os.path.exists(self.__conf.fullDumpFile):
                    with open(self.__conf.fullDumpFile, 'rb') as f:
                        conn.send(f.read())
                else:
                    conn.send('')
                return True
            elif message[0] == 'is_alive':
                conn.send(str(self.is_alive()))
                return True
            elif message[0] == 'is_ready':
                conn.send(str(self.isReady()))
                return True
        except Exception as e:
            conn.send(str(e))
            return True

    # index serializer
    def __serialize(self, filename, raft_data):
        with self.__lock:
            try:
                self.__logger.info('serializer has started')

                # store the index files and raft logs to the snapshot file
                with zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as f:
                    for index_name in self.get_index_names():
                        # self.__commit_index(index_name)
                        self.__optimize_index(index_name)

                        with self.__get_writer(index_name).lock():
                            # index files
                            for index_filename in self.get_index_files(index_name):
                                if self.__index_configs.get(index_name).get_storage_type() == "ram":
                                    with self.__ram_storage.open_file(index_filename) as r:
                                        f.writestr(index_filename, r.read())
                                else:
                                    f.write(os.path.join(self.__file_storage.folder, index_filename), index_filename)
                                self.__logger.debug('{0} has stored in {1}'.format(index_filename, filename))

                            # index config file
                            f.write(os.path.join(self.__file_storage.folder, self.get_index_config_file(index_name)),
                                    self.get_index_config_file(index_name))
                            self.__logger.debug(
                                '{0} has stored in {1}'.format(self.get_index_config_file(index_name), filename))

                    f.writestr('raft.bin', pickle.dumps(raft_data))
                self.__logger.info('snapshot has created')
            except Exception as ex:
                self.__logger.error('failed to create snapshot: {0}'.format(ex))
            finally:
                self.__logger.info('serializer has stopped')

    # index deserializer
    def __deserialize(self, filename):
        raft_data = None

        with self.__lock:
            try:
                self.__logger.info('deserializer has started')

                with zipfile.ZipFile(filename, 'r') as zf:
                    # get file names in snapshot file
                    filenames = list(zf.namelist())

                    # get index names in snapshot file
                    index_names = []
                    pattern_toc = re.compile(r'^_(.+)_\d+\.toc$')
                    for filename in filenames:
                        match = pattern_toc.search(filename)
                        if match and match.group(1) not in index_names:
                            index_names.append(match.group(1))

                    for index_name in index_names:
                        # extract the index config first
                        zf.extract(self.get_index_config_file(index_name), path=self.__file_storage.folder)
                        index_config = pickle.loads(zf.read(self.get_index_config_file(index_name)))

                        # get index files
                        pattern_toc = re.compile(r'^_{0}_(\d+)\..+$'.format(index_name))  # ex) _myindex_0.toc
                        pattern_seg = re.compile(
                            r'^{0}_([a-z0-9]+)\..+$'.format(index_name))  # ex) myindex_zseabukc2nbpvh0u.seg
                        pattern_lock = re.compile(r'^{0}_WRITELOCK$'.format(index_name))  # ex) myindex_WRITELOCK
                        index_files = []
                        for file_name in filenames:
                            if re.match(pattern_toc, file_name):
                                index_files.append(file_name)
                            elif re.match(pattern_seg, file_name):
                                index_files.append(file_name)
                            elif re.match(pattern_lock, file_name):
                                index_files.append(file_name)

                        # extract the index files
                        for index_file in index_files:
                            if index_config.get_storage_type() == 'ram':
                                with self.__ram_storage.create_file(index_file) as r:
                                    r.write(zf.read(index_file))
                            else:
                                zf.extract(index_file, path=self.__file_storage.folder)

                            self.__logger.debug('{0} has restored from {1}'.format(index_file, filename))

                        self.__logger.info('{0} has restored'.format(index_name))

                    # extract the raft data
                    raft_data = pickle.loads(zf.read('raft.bin'))
                    self.__logger.info('raft.bin has restored')
            except Exception as ex:
                self.__logger.error('failed to restore indices: {0}'.format(ex))
            finally:
                self.__logger.info('deserializer has stopped')

        return raft_data

    def get_index_files(self, index_name):
        index_files = []

        pattern_toc = re.compile(r'^_{0}_(\d+)\..+$'.format(index_name))  # ex) _myindex_0.toc
        pattern_seg = re.compile(r'^{0}_([a-z0-9]+)\..+$'.format(index_name))  # ex) myindex_zseabukc2nbpvh0u.seg
        pattern_lock = re.compile(r'^{0}_WRITELOCK$'.format(index_name))  # ex) myindex_WRITELOCK

        if self.__index_configs.get(index_name).get_storage_type() == "ram":
            storage = self.__ram_storage
        else:
            storage = self.__file_storage

        for file_name in list(storage.list()):
            if re.match(pattern_toc, file_name):
                index_files.append(file_name)
            elif re.match(pattern_seg, file_name):
                index_files.append(file_name)
            elif re.match(pattern_lock, file_name):
                index_files.append(file_name)

        return index_files

    def get_index_config_file(self, index_name):
        return '{0}_CONFIG'.format(index_name)

    def get_index_names(self):
        index_names = []

        pattern_toc = re.compile(r'^_(.+)_\d+\.toc$')

        for filename in list(self.__file_storage.list()):
            match = pattern_toc.search(filename)
            if match and match.group(1) not in index_names:
                index_names.append(match.group(1))
        for filename in list(self.__ram_storage.list()):
            match = pattern_toc.search(filename)
            if match and match.group(1) not in index_names:
                index_names.append(match.group(1))

        return index_names

    def is_index_exist(self, index_name):
        return self.__file_storage.index_exists(indexname=index_name) or self.__ram_storage.index_exists(
            indexname=index_name)

    def is_index_open(self, index_name):
        return index_name in self.__indices

    @replicated
    def open_index(self, index_name, index_config=None):
        return self.__open_index(index_name, index_config=index_config)

    def __open_index(self, index_name, index_config=None):
        start_time = time.time()

        index = None

        try:
            # open the index
            index = self.__indices.get(index_name)
            if index is None:
                self.__logger.info('opening {0}'.format(index_name))

                if index_config is None:
                    # set saved index config
                    with open(os.path.join(self.__file_storage.folder, self.get_index_config_file(index_name)),
                              'rb') as f:
                        self.__index_configs[index_name] = pickle.loads(f.read())
                else:
                    # set given index config
                    self.__index_configs[index_name] = index_config

                if self.__index_configs[index_name].get_storage_type() == 'ram':
                    index = self.__ram_storage.open_index(indexname=index_name,
                                                          schema=self.__index_configs[index_name].get_schema())
                else:
                    index = self.__file_storage.open_index(indexname=index_name,
                                                           schema=self.__index_configs[index_name].get_schema())
                self.__indices[index_name] = index

                self.__logger.info('{0} has opened'.format(index_name))

                # open the index writer
                self.__open_writer(index_name)
        except Exception as ex:
            self.__logger.error('failed to open {0}: {1}'.format(index_name, ex))
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return index

    @replicated
    def close_index(self, index_name):
        return self.__close_index(index_name)

    def __close_index(self, index_name):
        start_time = time.time()

        index = None

        try:
            # close the index writer
            self.__close_writer(index_name)

            # close the index
            index = self.__indices.pop(index_name)
            if index is not None:
                self.__logger.info('closing {0}'.format(index_name))
                index.close()
                self.__logger.info('{0} has closed'.format(index_name))
        except Exception as ex:
            self.__logger.error('failed to close {0}: {1}'.format(index_name, ex))
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return index

    @replicated
    def create_index(self, index_name, index_config):
        return self.__create_index(index_name, index_config)

    def __create_index(self, index_name, index_config):
        start_time = time.time()

        index = None

        try:
            if self.is_index_exist(index_name):
                # open the index
                index = self.__open_index(index_name, index_config=index_config)
            else:
                self.__logger.info('creating {0}'.format(index_name))

                # set index config
                self.__index_configs[index_name] = index_config

                # create the index
                if self.__index_configs[index_name].get_storage_type() == 'ram':
                    index = self.__ram_storage.create_index(self.__index_configs[index_name].get_schema(),
                                                            indexname=index_name)
                else:
                    index = self.__file_storage.create_index(self.__index_configs[index_name].get_schema(),
                                                             indexname=index_name)
                self.__indices[index_name] = index
                self.__logger.info('{0} has created'.format(index_name))

                # save the index config
                with open(os.path.join(self.__file_storage.folder, self.get_index_config_file(index_name)),
                          'wb') as f:
                    f.write(pickle.dumps(index_config))

                # open the index writer
                self.__open_writer(index_name)
        except Exception as ex:
            self.__logger.error('failed to create {0}: {1}'.format(index_name, ex))
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return index

    @replicated
    def delete_index(self, index_name):
        return self.__delete_index(index_name)

    def __delete_index(self, index_name):
        start_time = time.time()

        # close index
        index = self.__close_index(index_name)

        try:
            self.__logger.info('deleting {0}'.format(index_name))

            # delete index files
            for filename in self.get_index_files(index_name):
                self.__file_storage.delete_file(filename)
                self.__logger.info('{0} was deleted'.format(filename))

            self.__logger.info('{0} has deleted'.format(index_name))

            # delete the index config
            self.__index_configs.pop(index_name, None)
            os.remove(os.path.join(self.__file_storage.folder, self.get_index_config_file(index_name)))
        except Exception as ex:
            self.__logger.error('failed to delete {0}: {1}'.format(index_name, ex))
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return index

    def get_index(self, index_name):
        start_time = time.time()

        try:
            index = self.__indices.get(index_name)
        except Exception as ex:
            raise ex
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return index

    @replicated
    def commit_index(self, index_name):
        return self.__commit_index(index_name)

    def __commit_index(self, index_name):
        start_time = time.time()

        success = False

        try:
            self.__logger.info('committing {0}'.format(index_name))

            self.__get_writer(index_name).commit()

            self.__logger.info('{0} has committed'.format(index_name))

            success = True
        except Exception as ex:
            self.__logger.error('failed to commit index {0}: {1}'.format(index_name, ex))
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return success

    @replicated
    def rollback_index(self, index_name):
        return self.__rollback_index(index_name)

    def __rollback_index(self, index_name):
        start_time = time.time()

        success = False

        try:
            self.__logger.info('rolling back {0}'.format(index_name))

            self.__get_writer(index_name).rollback()

            self.__logger.info('{0} has rolled back'.format(index_name))

            success = True
        except Exception as ex:
            self.__logger.error('failed to rollback index {0}: {1}'.format(index_name, ex))
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return success

    @replicated
    def optimize_index(self, index_name):
        return self.__optimize_index(index_name)

    def __optimize_index(self, index_name):
        start_time = time.time()

        success = False

        try:
            self.__logger.info('optimizing {0}'.format(index_name))

            self.__writers.get(index_name).optimize()

            self.__logger.info('{0} has optimized'.format(index_name))

            success = True
        except Exception as ex:
            self.__logger.error('failed to optimize {0}: {1}'.format(index_name, ex))
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return success

    def get_doc_count(self, index_name):
        try:
            cnt = self.__indices.get(index_name).doc_count()
        except Exception as ex:
            raise ex

        return cnt

    def get_schema(self, index_name):
        try:
            schema = self.__indices.get(index_name).schema
        except Exception as ex:
            raise ex

        return schema

    def __open_writer(self, index_name):
        try:
            self.__logger.info('opening writer for {0}'.format(index_name))

            # open the index writer
            writer = IndexWriter(self.__indices.get(index_name),
                                 procs=self.__index_configs.get(index_name).get_writer_processors(),
                                 batchsize=self.__index_configs.get(index_name).get_writer_batch_size(),
                                 multisegment=self.__index_configs.get(index_name).get_writer_multi_segment(),
                                 period=self.__index_configs.get(index_name).get_writer_auto_commit_period(),
                                 limit=self.__index_configs.get(index_name).get_writer_auto_commit_limit(),
                                 logger=self.__logger)
            self.__writers[index_name] = writer

            self.__logger.info('writer for {0} has opened'.format(index_name))
        except Exception as ex:
            raise ex

        return writer

    def __close_writer(self, index_name):
        try:
            self.__logger.info('closing writer for {0}'.format(index_name))

            # close the index writer
            writer = self.__writers.pop(index_name) if index_name in self.__writers else None
            if writer is not None and not writer.is_closed():
                writer.close()

            self.__logger.info('writer for {0} has closed'.format(index_name))
        except Exception as ex:
            raise ex

        return writer

    def __get_writer(self, index_name):
        return self.__writers.get(index_name, None)

    def __get_searcher(self, index_name, weighting=None):
        try:
            if weighting is None:
                searcher = self.__indices.get(index_name).searcher()
            else:
                searcher = self.__indices.get(index_name).searcher(weighting=weighting)
        except Exception as ex:
            raise ex

        return searcher

    @replicated
    def put_document(self, index_name, doc_id, fields):
        return self.__put_document(index_name, doc_id, fields)

    def __put_document(self, index_name, doc_id, fields):
        doc = copy.deepcopy(fields)
        doc[self.__index_configs.get(index_name).get_doc_id_field()] = doc_id

        return self.__put_documents(index_name, [doc])

    @replicated
    def put_documents(self, index_name, docs):
        return self.__put_documents(index_name, docs)

    def __put_documents(self, index_name, docs):
        start_time = time.time()

        try:
            self.__logger.info('putting documents to {0}'.format(index_name))

            count = self.__get_writer(index_name).update_documents(docs)

            self.__logger.info('{0} documents has put to {1}'.format(count, index_name))
        except Exception as ex:
            self.__logger.error('failed to put documents to {0}: {1}'.format(index_name, ex))
            count = -1
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return count

    def get_document(self, index_name, doc_id):
        start_time = time.time()

        try:
            results_page = self.search_documents(index_name, doc_id,
                                                 self.__index_configs.get(index_name).get_doc_id_field(), 1,
                                                 page_len=1)
            if results_page.total > 0:
                self.__logger.info('{0} was got from {1}'.format(doc_id, index_name))
            else:
                self.__logger.info('{0} did not exist in {1}'.format(doc_id, index_name))
        except Exception as ex:
            raise ex
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return results_page

    @replicated
    def delete_document(self, index_name, doc_id):
        return self.__delete_document(index_name, doc_id)

    def __delete_document(self, index_name, doc_id):
        return self.__delete_documents(index_name, [doc_id])

    @replicated
    def delete_documents(self, index_name, doc_ids):
        return self.__delete_documents(index_name, doc_ids)

    def __delete_documents(self, index_name, doc_ids):
        start_time = time.time()

        try:
            self.__logger.info('deleting documents from {0}'.format(index_name))

            count = self.__get_writer(index_name).delete_documents(doc_ids, doc_id_field=self.__index_configs.get(
                index_name).get_doc_id_field())

            self.__logger.info('{0} documents has deleted from {1}'.format(count, index_name))
        except Exception as ex:
            self.__logger.error('failed to delete documents in bulk to {0}: {1}'.format(index_name, ex))
            count = -1
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return count

    def search_documents(self, index_name, query, search_field, page_num, page_len=10, weighting=None, **kwargs):
        start_time = time.time()

        try:
            searcher = self.__get_searcher(index_name, weighting=weighting)
            query_parser = QueryParser(search_field, self.get_schema(index_name))
            query_obj = query_parser.parse(query)
            results_page = searcher.search_page(query_obj, page_num, pagelen=page_len, **kwargs)
            self.__logger.info('{0} documents ware searched from {1}'.format(results_page.total, index_name))
        except Exception as ex:
            raise ex
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return results_page

    @replicated
    def create_snapshot(self):
        self.__create_snapshot()

    def __create_snapshot(self):
        start_time = time.time()

        try:
            self.forceLogCompaction()
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

    def get_snapshot_file_name(self):
        return self.__conf.fullDumpFile

    def is_snapshot_exist(self):
        return os.path.exists(self.get_snapshot_file_name())

    def open_snapshot_file(self):
        try:
            file = open(self.get_snapshot_file_name(), mode='rb')
        except Exception as ex:
            raise ex

        return file
