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
import os
import re
import time
import zipfile
from concurrent import futures
from http import HTTPStatus
from logging import getLogger
from threading import RLock, Thread, Timer

import grpc
import pysyncobj.pickle as pickle
import requests
from prometheus_client.core import CollectorRegistry, Counter, Gauge, Histogram
from pysyncobj import replicated, SyncObjConf
from whoosh.filedb.filestore import FileStorage, RamStorage
from whoosh.qparser import QueryParser

from cockatrice import NAME
from cockatrice.indexer_grpc import IndexGRPCServicer
from cockatrice.indexer_http import IndexHTTPServicer
from cockatrice.protobuf.index_pb2_grpc import add_IndexServicer_to_server
from cockatrice.util.http import HTTPServer
from cockatrice.util.raft import add_node, get_leader, get_metadata, get_peers, RAFT_DATA_FILE, RaftNode


class Indexer(RaftNode):
    def __init__(self, host='localhost', port=7070, seed_addr=None, conf=SyncObjConf(),
                 data_dir='/tmp/cockatrice/index', grpc_port=5050, grpc_max_workers=10, http_port=8080,
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
        self.__metrics_core_documents = Gauge(
            '{0}_indexer_index_documents'.format(NAME),
            'The number of documents.',
            [
                'index_name',
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_requests_total = Counter(
            '{0}_indexer_requests_total'.format(NAME),
            'The number of requests.',
            [
                'func'
            ],
            registry=self.__metrics_registry
        )
        self.__metrics_requests_duration_seconds = Histogram(
            '{0}_indexer_requests_duration_seconds'.format(NAME),
            'The invocation duration in seconds.',
            [
                'func'
            ],
            registry=self.__metrics_registry
        )

        self.__self_addr = '{0}:{1}'.format(self.__host, self.__port)
        self.__peer_addrs = [] if self.__seed_addr is None else get_peers(bind_addr=self.__seed_addr, timeout=10)
        self.__other_addrs = [peer_addr for peer_addr in self.__peer_addrs if peer_addr != self.__self_addr]
        self.__conf.serializer = self.__serialize
        self.__conf.deserializer = self.__deserialize
        self.__conf.validate()

        self.__indices = {}
        self.__index_configs = {}
        self.__writers = {}
        self.__lock = RLock()

        # create data dir
        os.makedirs(self.__data_dir, exist_ok=True)
        self.__file_storage = FileStorage(self.__data_dir, supports_mmap=True, readonly=False, debug=False)
        self.__ram_storage = RamStorage()

        # if seed addr specified and self node does not exist in the cluster, add self node to the cluster
        if self.__seed_addr is not None and self.__self_addr not in self.__peer_addrs:
            Thread(target=add_node,
                   kwargs={'node_name': self.__self_addr, 'bind_addr': self.__seed_addr, 'timeout': 10}).start()

        # copy snapshot from the leader node
        if self.__seed_addr is not None:
            try:
                metadata = get_metadata(bind_addr=get_leader(bind_addr=self.__seed_addr, timeout=10), timeout=10)
                response = requests.get('http://{0}/snapshot'.format(metadata['http_addr']))
                if response.status_code == HTTPStatus.OK:
                    with open(self.__conf.fullDumpFile, 'wb') as f:
                        f.write(response.content)
            except Exception as ex:
                self.__logger.error('failed to copy snapshot: {0}'.format(ex))

        # start node
        metadata = {
            'grpc_addr': '{0}:{1}'.format(self.__host, self.__grpc_port),
            'http_addr': '{0}:{1}'.format(self.__host, self.__http_port)
        }
        super(Indexer, self).__init__(self.__self_addr, self.__peer_addrs, conf=self.__conf, metadata=metadata)
        self.__logger.info('state machine has started')
        while not self.isReady():
            # recovering data
            self.__logger.debug('waiting for cluster ready')
            time.sleep(1)
        self.__logger.info('cluster ready')

        # open existing indices on startup
        for index_name in self.get_index_names():
            self.__open_index(index_name, index_config=None)

        # record index metrics timer
        self.metrics_timer = Timer(10, self.__record_index_metrics)
        self.metrics_timer.start()

        # start gRPC
        self.__grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=self.__grpc_max_workers))
        add_IndexServicer_to_server(
            IndexGRPCServicer(self, logger=self.__logger, metrics_registry=self.__metrics_registry),
            self.__grpc_server)
        self.__grpc_server.add_insecure_port('{0}:{1}'.format(self.__host, self.__grpc_port))
        self.__grpc_server.start()
        self.__logger.info('gRPC server has started')

        # start HTTP server
        self.__http_servicer = IndexHTTPServicer(self, self.__logger, self.__http_logger, self.__metrics_registry)
        self.__http_server = HTTPServer(self.__host, self.__http_port, self.__http_servicer)
        self.__http_server.start()
        self.__logger.info('HTTP server has started')

        self.__logger.info('indexer has started')

    def stop(self):
        # stop HTTP server
        self.__http_server.stop()
        self.__logger.info('HTTP server has stopped')

        # stop gRPC server
        self.__grpc_server.stop(grace=0.0)
        self.__logger.info('gRPC server has stopped')

        self.metrics_timer.cancel()

        # close indices
        for index_name in list(self.__indices.keys()):
            self.__close_index(index_name)

        self.destroy()

        self.__logger.info('index core has stopped')

    def __record_index_metrics(self):
        for index_name in list(self.__indices.keys()):
            try:
                self.__metrics_core_documents.labels(index_name=index_name).set(self.get_doc_count(index_name))
            except Exception as ex:
                self.__logger.error(ex)

    def __record_metrics(self, start_time, func_name):
        self.__metrics_requests_total.labels(
            func=func_name
        ).inc()

        self.__metrics_requests_duration_seconds.labels(
            func=func_name
        ).observe(time.time() - start_time)

    # index serializer
    def __serialize(self, filename, raft_data):
        with self.__lock:
            try:
                self.__logger.info('serializer has started')

                # store the index files and raft logs to the snapshot file
                with zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as f:
                    for index_name in self.get_index_names():
                        self.__commit_index(index_name)

                        # with self.__get_writer(index_name).writelock:
                        # with self.__indices[index_name].lock('WRITELOCK'):
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

                    # store the raft data
                    f.writestr(RAFT_DATA_FILE, pickle.dumps(raft_data))
                    self.__logger.info('{0} has restored'.format(RAFT_DATA_FILE))
                self.__logger.info('snapshot has created')
            except Exception as ex:
                self.__logger.error('failed to create snapshot: {0}'.format(ex))
            finally:
                self.__logger.info('serializer has stopped')

    # index deserializer
    def __deserialize(self, filename):
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
                    raft_data = pickle.loads(zf.read(RAFT_DATA_FILE))
                    self.__logger.info('{0} has restored'.format(RAFT_DATA_FILE))
                    return raft_data
            except Exception as ex:
                self.__logger.error('failed to restore indices: {0}'.format(ex))
            finally:
                self.__logger.info('deserializer has stopped')

    def is_healthy(self):
        return self.isHealthy()

    def is_alive(self):
        return self.isAlive()

    def is_ready(self):
        return self.isReady()

    def get_addr(self):
        return self.__self_addr

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

    @staticmethod
    def get_index_config_file(index_name):
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
            self.__record_metrics(start_time, 'open_index')

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
            self.__record_metrics(start_time, 'close_index')

        return index

    @replicated
    def create_index(self, index_name, index_config):
        return self.__create_index(index_name, index_config)

    def __create_index(self, index_name, index_config):
        if self.is_index_exist(index_name):
            # open the index
            return self.__open_index(index_name, index_config=index_config)

        start_time = time.time()

        index = None

        with self.__lock:
            try:
                self.__logger.info('creating {0}'.format(index_name))

                # set index config
                self.__index_configs[index_name] = index_config

                self.__logger.debug(self.__index_configs[index_name].get_storage_type())

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
                self.__record_metrics(start_time, 'create_index')

        return index

    @replicated
    def delete_index(self, index_name):
        return self.__delete_index(index_name)

    def __delete_index(self, index_name):
        # close index
        index = self.__close_index(index_name)

        start_time = time.time()

        with self.__lock:
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
                self.__record_metrics(start_time, 'delete_index')

        return index

    def get_index(self, index_name):
        return self.__get_index(index_name)

    def __get_index(self, index_name):
        start_time = time.time()

        try:
            index = self.__indices.get(index_name)
        except Exception as ex:
            raise ex
        finally:
            self.__record_metrics(start_time, 'get_index')

        return index

    def __open_writer(self, index_name):
        writer = None

        try:
            writer = self.__writers.get(index_name, None)
            if writer is None or writer.is_closed:
                self.__logger.info('opening writer for {0}'.format(index_name))
                writer = self.__indices.get(index_name).writer()
                self.__writers[index_name] = writer
                self.__logger.info('writer for {0} has opened'.format(index_name))
        except Exception as ex:
            self.__logger.error('failed to open writer for {0}: {1}'.format(index_name, ex))

        return writer

    def __close_writer(self, index_name):
        writer = None

        try:
            # close the index
            writer = self.__writers.pop(index_name, None)
            if writer is not None:
                self.__logger.info('closing writer for {0}'.format(index_name))
                writer.commit()
                self.__logger.info('writer for {0} has closed'.format(index_name))
        except Exception as ex:
            self.__logger.error('failed to close writer for {0}: {1}'.format(index_name, ex))

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
    def commit_index(self, index_name):
        return self.__commit_index(index_name)

    def __commit_index(self, index_name):
        start_time = time.time()

        success = False

        with self.__lock:
            try:
                self.__logger.info('committing {0}'.format(index_name))

                self.__get_writer(index_name).commit()
                self.__open_writer(index_name)  # reopen writer

                self.__logger.info('{0} has committed'.format(index_name))

                success = True
            except Exception as ex:
                self.__logger.error('failed to commit index {0}: {1}'.format(index_name, ex))
            finally:
                self.__record_metrics(start_time, 'commit_index')

        return success

    @replicated
    def rollback_index(self, index_name):
        return self.__rollback_index(index_name)

    def __rollback_index(self, index_name):
        start_time = time.time()

        success = False

        with self.__lock:
            try:
                self.__logger.info('rolling back {0}'.format(index_name))

                self.__get_writer(index_name).cancel()
                self.__open_writer(index_name)  # reopen writer

                self.__logger.info('{0} has rolled back'.format(index_name))

                success = True
            except Exception as ex:
                self.__logger.error('failed to rollback index {0}: {1}'.format(index_name, ex))
            finally:
                self.__record_metrics(start_time, 'rollback_index')

        return success

    @replicated
    def optimize_index(self, index_name):
        return self.__optimize_index(index_name)

    def __optimize_index(self, index_name):
        start_time = time.time()

        success = False

        with self.__lock:
            try:
                self.__logger.info('optimizing {0}'.format(index_name))

                self.__get_writer(index_name).commit(optimize=True, merge=False)
                self.__open_writer(index_name)  # reopen writer

                self.__logger.info('{0} has optimized'.format(index_name))

                success = True
            except Exception as ex:
                self.__logger.error('failed to optimize {0}: {1}'.format(index_name, ex))
            finally:
                self.__record_metrics(start_time, 'optimize_index')

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

        with self.__lock:
            try:
                self.__logger.info('putting documents to {0}'.format(index_name))

                # count = self.__get_writer(index_name).update_documents(docs)

                count = 0
                for doc in docs:
                    self.__get_writer(index_name).update_document(**doc)
                    count += 1

                self.__logger.info('{0} documents has put to {1}'.format(count, index_name))
            except Exception as ex:
                self.__logger.error('failed to put documents to {0}: {1}'.format(index_name, ex))
                count = -1
            finally:
                self.__record_metrics(start_time, 'put_documents')

        return count

    def get_document(self, index_name, doc_id):
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

        with self.__lock:
            try:
                self.__logger.info('deleting documents from {0}'.format(index_name))

                # count = self.__get_writer(index_name).delete_documents(doc_ids, doc_id_field=self.__index_configs.get(
                #     index_name).get_doc_id_field())

                count = 0
                for doc_id in doc_ids:
                    count += self.__get_writer(index_name).delete_by_term(
                        self.__index_configs.get(index_name).get_doc_id_field(), doc_id)

                self.__logger.info('{0} documents has deleted from {1}'.format(count, index_name))
            except Exception as ex:
                self.__logger.error('failed to delete documents in bulk to {0}: {1}'.format(index_name, ex))
                count = -1
            finally:
                self.__record_metrics(start_time, 'delete_documents')

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
            self.__record_metrics(start_time, 'search_documents')

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
        with self.__lock:
            try:
                file = open(self.get_snapshot_file_name(), mode='rb')
            except Exception as ex:
                raise ex

        return file
