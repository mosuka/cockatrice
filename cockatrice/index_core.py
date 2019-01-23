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
from whoosh.filedb.filestore import FileStorage
from whoosh.qparser import QueryParser

from cockatrice import NAME
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

        self.__writer_locks = {}
        self.__writers = {}
        self.__writer_timers = {}

        self.__file_storage = None

        os.makedirs(self.__index_dir, exist_ok=True)
        self.__file_storage = FileStorage(self.__index_dir, supports_mmap=True, readonly=False, debug=False)

        super(IndexCore, self).__init__(self.__bind_addr, self.__peer_addrs, conf=self.__conf)
        self.__logger.info('index core started')

        # waiting for the preparation to be completed
        while not self.isReady():
            # recovering data
            self.__logger.debug('waiting for the cluster ready')
            time.sleep(1)
        self.__logger.info('index core ready')

        # open existing indices on startup
        for index_name in self.get_index_names():
            self.__open_index(index_name, schema=None)

        self.metrics_timer = RepeatedTimer(10, self.__record_metrics)
        self.metrics_timer.start()

    def stop(self):
        self.metrics_timer.cancel()

        # close indices
        for index_name in list(self.__indices.keys()):
            self.__close_index(index_name)

        self.destroy()

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

        return

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
                self.__logger.info('serializer has been started')

                # store the index files and raft logs to the snapshot file
                with zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as f:
                    for index_name in self.get_index_names():
                        with self.__writer_locks.get(index_name):
                            self.__commit_index(index_name)

                            for index_filename in self.get_index_file_names(index_name=index_name):
                                abs_index_path = os.path.join(self.__file_storage.folder, index_filename)
                                f.write(abs_index_path, index_filename)
                                self.__logger.debug('{0} has been stored in {1}'.format(abs_index_path, filename))
                    f.writestr('raft.bin', pickle.dumps(raft_data))
            except Exception as ex:
                self.__logger.error(ex)
            self.__logger.info('serializer has been stopped')

    # index deserializer
    def __deserialize(self, filename):
        with self.__lock:
            try:
                self.__logger.info('deserializer has been started')
                with zipfile.ZipFile(filename, 'r') as zf:
                    for member in zf.namelist():
                        if member != 'raft.bin':
                            zf.extract(member, path=self.__file_storage.folder)
                            self.__logger.debug('{0} has been restored from {1}'.format(
                                os.path.join(self.__file_storage.folder, member), filename))
                    return pickle.loads(zf.read('raft.bin'))

            except Exception as ex:
                self.__logger.error(ex)
            finally:
                self.__logger.info('deserializer has been stopped')

    def get_index_file_names(self, index_name=None):
        index_files = []

        pattern_toc = re.compile(r'^_{0}_(\d+)\.toc$'.format(index_name))  # ex) _myindex_0.toc
        pattern_seg = re.compile(r'^{0}_([a-z0-9]+)\.seg$'.format(index_name))  # ex) myindex_zseabukc2nbpvh0u.seg
        pattern_lock = re.compile(r'^{0}_WRITELOCK$'.format(index_name))  # ex) myindex_WRITELOCK

        for file_name in list(self.__file_storage.list()):
            if index_name is not None:
                if re.match(pattern_toc, file_name):
                    index_files.append(file_name)
                elif re.match(pattern_seg, file_name):
                    index_files.append(file_name)
                elif re.match(pattern_lock, file_name):
                    index_files.append(file_name)
            else:
                index_files.append(file_name)

        return index_files

    def get_index_names(self):
        index_names = []

        pattern_toc = re.compile(r'^_(.+)_\d+\.toc$')
        for filename in list(self.__file_storage.list()):
            match = pattern_toc.search(filename)
            if match and match.group(1) not in index_names:
                index_names.append(match.group(1))

        return index_names

    def is_index_exist(self, index_name):
        return self.__file_storage.index_exists(indexname=index_name)

    def is_index_open(self, index_name):
        return index_name in self.__indices

    @replicated
    def open_index(self, index_name, schema=None):
        start_time = time.time()

        try:
            index = self.__open_index(index_name, schema=schema)
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return index

    def __open_index(self, index_name, schema=None):
        index = None

        try:
            # open the index
            index = self.__indices.get(index_name)
            if index is None:
                index = self.__file_storage.open_index(indexname=index_name, schema=schema)
                self.__indices[index_name] = index
                self.__logger.info('{0} was opened'.format(index_name))

            # open the index writer
            self.__open_writer(index_name)

        except Exception as ex:
            self.__logger.error('failed to open {0}: {1}'.format(index_name, ex))

        return index

    @replicated
    def close_index(self, index_name):
        start_time = time.time()

        try:
            index = self.__close_index(index_name)
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return index

    def __close_index(self, index_name):
        index = None

        try:
            # stop timers
            if index_name in self.__writer_timers:
                timer = self.__writer_timers.pop(index_name)
                if timer is not None:
                    timer.cancel()
                    self.__logger.info('auto-commit timer for {0} was stopped'.format(index_name))

            # close the index writer
            self.__close_writer(index_name)

            # close the index
            index = self.__indices.pop(index_name)
            if index is not None:
                index.close()
                self.__logger.info('{0} was closed'.format(index_name))
        except Exception as ex:
            self.__logger.error('failed to close {0}: {1}'.format(index_name, ex))

        return index

    @replicated
    def create_index(self, index_name, schema):
        start_time = time.time()

        try:
            index = self.__create_index(index_name, schema)
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return index

    def __create_index(self, index_name, schema):
        with self.__lock:
            index = None

            try:
                if self.is_index_exist(index_name):
                    # open the index
                    index = self.__open_index(index_name, schema=schema)
                else:
                    # create the index
                    index = self.__file_storage.create_index(schema, indexname=index_name)
                    self.__indices[index_name] = index
                    self.__logger.info('{0} was created'.format(index_name))

                    # open the index writer
                    self.__open_writer(index_name)

                    # start the timer
                    self.__writer_timers[index_name] = RepeatedTimer(10, self.commit_index, (index_name,))
                    self.__writer_timers.get(index_name).start()
                    self.__logger.info('auto-commit timer for {0} was started'.format(index_name))

                # save the schema in the index dir
                with open(os.path.join(self.__file_storage.folder, '{0}.schema'.format(index_name)), 'wb') as f:
                    f.write(pickle.dumps(schema))
            except Exception as ex:
                self.__logger.error('failed to create {0}: {1}'.format(index_name, ex))

        return index

    @replicated
    def delete_index(self, index_name):
        start_time = time.time()

        try:
            index = self.__delete_index(index_name)
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return index

    def __delete_index(self, index_name):
        with self.__lock:
            # close index
            index = self.__close_index(index_name)

            # delete index files
            try:
                for filename in self.get_index_file_names(index_name=index_name):
                    self.__file_storage.delete_file(filename)
                    self.__logger.info('{0} was deleted'.format(filename))
                self.__logger.info('{0} was deleted'.format(index_name))
            except Exception as ex:
                self.__logger.error('failed to delete {0}: {1}'.format(index_name, ex))

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
        start_time = time.time()

        try:
            success = self.__commit_index(index_name)
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return success

    def __commit_index(self, index_name):
        with self.__writer_locks[index_name]:
            success = False

            try:
                self.__get_writer(index_name).commit()
                self.__logger.info('{0} was committed'.format(index_name))
                success = True

                # reopen index writer
                self.__open_writer(index_name)
            except Exception as ex:
                self.__logger.error('failed to commit index {0}: {1}'.format(index_name, ex))

        return success

    @replicated
    def rollback_index(self, index_name):
        start_time = time.time()

        try:
            success = self.__rollback_index(index_name)
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return success

    def __rollback_index(self, index_name):
        with self.__writer_locks[index_name]:
            success = False

            try:
                self.__get_writer(index_name).cancel()
                self.__logger.info('{0} was rolled back'.format(index_name))
                success = True
            except Exception as ex:
                self.__logger.error('failed to rollback index {0}: {1}'.format(index_name, ex))

        return success

    @replicated
    def optimize_index(self, index_name):
        start_time = time.time()

        try:
            success = self.__optimize_index(index_name)
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return success

    def __optimize_index(self, index_name):
        with self.__writer_locks[index_name]:
            success = False

            try:
                self.__writers.get(index_name).commit(optimize=True)
                self.__logger.info('{0} was optimized'.format(index_name))
                success = True
            except Exception as ex:
                self.__logger.error('failed to optimize {0}: {1}'.format(index_name, ex))

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
            # open the index writer
            writer = self.__indices.get(index_name).writer(proc=16, batchsize=10)
            # writer = BufferedWriter(self.__indices.get(index_name),
            #                         period=self.get_schema(index_name).get_auto_commit_period(),
            #                         limit=self.get_schema(index_name).get_auto_commit_limit())
            self.__writers[index_name] = writer
            self.__logger.info('writer for {0} was opened'.format(index_name))

            self.__writer_locks[index_name] = threading.RLock()
        except Exception as ex:
            raise ex

        return writer

    def __close_writer(self, index_name):
        try:
            # close the index writer
            writer = self.__writers.pop(index_name) if index_name in self.__writers else None
            if writer is not None and not writer.is_closed:
                writer.commit()  # SegmentWriter
            self.__logger.info('writer for {0} was closed'.format(index_name))

            self.__writer_locks.pop(index_name)
        except Exception as ex:
            raise ex

        return writer

    def __get_writer(self, index_name):
        try:
            writer = self.__writers.get(index_name)
        except Exception as ex:
            raise ex

        return writer

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
        start_time = time.time()

        try:
            count = self.__put_document(index_name, doc_id, fields)
        except Exception as ex:
            count = -1
            self.__logger.error('failed to put {0} in {1}: {2}'.format(doc_id, index_name, ex))
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return count

    def __put_document(self, index_name, doc_id, fields):
        try:
            doc = copy.deepcopy(fields)
            doc[self.get_schema(index_name).get_doc_id_field()] = doc_id

            count = self.__put_documents(index_name, [doc])
            self.__logger.info('{0} was put in {1}'.format(doc_id, index_name))
        except Exception as ex:
            self.__logger.error('failed to put {0} in {1}: {2}'.format(doc_id, index_name, ex))
            raise ex

        return count

    def get_document(self, index_name, doc_id):
        start_time = time.time()

        try:
            results_page = self.search_documents(index_name, doc_id, self.get_schema(index_name).get_doc_id_field(), 1,
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
        start_time = time.time()

        try:
            count = self.__delete_document(index_name, doc_id)
        except Exception as ex:
            count = -1
            self.__logger.error('failed to delete {0} from {1}: {2}'.format(doc_id, index_name, ex))
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return count

    def __delete_document(self, index_name, doc_id):
        try:
            count = self.__delete_documents(index_name, [doc_id])
            self.__logger.info('{0} was deleted from {1}'.format(doc_id, index_name))
        except Exception as ex:
            self.__logger.error('failed to delete {0} from {1}: {2}'.format(doc_id, index_name, ex))
            raise ex

        return count

    @replicated
    def put_documents(self, index_name, docs):
        start_time = time.time()

        try:
            count = self.__put_documents(index_name, docs)
        except Exception as ex:
            count = -1
            self.__logger.error('failed to put documents in bulk to {0}: {1}'.format(index_name, ex))
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return count

    def __put_documents(self, index_name, docs):
        count = 0

        try:
            writer = self.__get_writer(index_name)
            for doc in docs:
                writer.update_document(**doc)
                self.__logger.debug(
                    '{0} ware put in {1}'.format(doc[self.get_schema(index_name).get_doc_id_field()], index_name))
                count = count + 1
            self.__logger.info('{0} documents ware put in {1}'.format(count, index_name))
        except Exception as ex:
            self.__logger.error('failed to put documents in bulk to {0}: {1}'.format(index_name, ex))
            raise ex

        return count

    @replicated
    def delete_documents(self, index_name, doc_ids):
        start_time = time.time()

        try:
            count = self.__delete_documents(index_name, doc_ids)
        except Exception as ex:
            count = -1
            self.__logger.error('failed to delete documents in bulk to {0}: {1}'.format(index_name, ex))
        finally:
            self.__record_core_metrics(start_time, inspect.getframeinfo(inspect.currentframe())[2])

        return count

    def __delete_documents(self, index_name, doc_ids):
        count = 0

        try:
            writer = self.__writers.get(index_name)
            for doc_id in doc_ids:
                count = count + writer.delete_by_term(self.get_schema(index_name).get_doc_id_field(), doc_id)
                self.__logger.debug('{0} ware deleted from {1}'.format(doc_id, index_name))
            self.__logger.info('{0} documents ware deleted from {1}'.format(count, index_name))
        except Exception as ex:
            self.__logger.error('failed to delete documents in bulk to {0}: {1}'.format(index_name, ex))
            raise ex

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
        self.forceLogCompaction()

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
