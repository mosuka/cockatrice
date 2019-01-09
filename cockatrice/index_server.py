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

import copy
import functools
import os
import re
import threading
import time
import zipfile
from logging import getLogger

import pysyncobj.pickle as pickle
from pysyncobj import replicated, SyncObj, SyncObjConf
from whoosh.filedb.filestore import FileStorage
from whoosh.qparser import QueryParser


class IndexServer(SyncObj):
    def __init__(self, host='localhost', port=7070, peer_addrs=[], conf=SyncObjConf(),
                 index_dir='/tmp/cockatrice/index', logger=getLogger()):
        self.__logger = logger

        self.__lock = threading.RLock()

        self.__bind_addr = '{0}:{1}'.format(host, port)
        self.__peer_addrs = peer_addrs
        self.__index_dir = index_dir
        self.__conf = conf
        self.__conf.serializer = self.__serialize
        self.__conf.deserializer = self.__deserialize
        self.__conf.validate()

        self.__indices = {}

        self.__file_storage = None

        os.makedirs(self.__index_dir, exist_ok=True)
        self.__file_storage = FileStorage(self.__index_dir, supports_mmap=True, readonly=False, debug=False)

        super(IndexServer, self).__init__(self.__bind_addr, self.__peer_addrs, conf=self.__conf)
        self.__logger.info('Server started')

        # open existing indices on startup
        self.__open_existing_indices()

        # waiting for the preparation to be completed
        while not self.isReady():
            # recovering data
            time.sleep(1)
        self.__logger.info('Server ready')

    def stop(self):
        for index_name in self.__indices.keys():
            self.close_index(index_name)
        self.destroy()

    def destroy(self):
        super().destroy()

    def isAlive(self):
        # TODO:　think about it later
        return True

    def get_addr(self):
        return self.__bind_addr

    def _SyncObj__onUtilityMessage(self, conn, message):
        try:
            if message[0] == 'status':
                conn.send(self.getStatus())
                return True
            elif message[0] == 'add':
                self.addNodeToCluster(message[1],
                                      callback=functools.partial(self.__utilityCallback, conn=conn, cmd='ADD',
                                                                 node=message[1]))
                return True
            elif message[0] == 'remove':
                if message[1] == self.__selfNodeAddr:
                    conn.send('FAIL REMOVE ' + message[1])
                else:
                    self.removeNodeFromCluster(message[1], callback=functools.partial(self.__utilityCallback, conn=conn,
                                                                                      cmd='REMOVE', node=message[1]))
                return True
            elif message[0] == 'set_version':
                self.setCodeVersion(message[1],
                                    callback=functools.partial(self.__utilityCallback, conn=conn, cmd='SET_VERSION',
                                                               node=str(message[1])))
                return True
            elif message[0] == 'get_snapshot':
                with open(self.__conf.fullDumpFile, 'rb') as f:
                    conn.send(f.read())
                return True
            elif message[0] == 'is_alive':
                conn.send(str(self.isAlive()))
                return True
            elif message[0] == 'is_ready':
                conn.send(str(self.isReady()))
                return True
        except Exception as e:
            conn.send(str(e))
            return True

    def __serialize(self, filename, raft_data):
        try:
            with self.__lock:
                with zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as f:
                    for index_filename in self.__file_storage:
                        abs_index_path = os.path.join(self.__file_storage.folder, index_filename)
                        f.write(abs_index_path, index_filename)
                    f.writestr('raft.bin', pickle.dumps(raft_data))
        except Exception as ex:
            self.__logger.error(ex)

    def __deserialize(self, filename):
        try:
            with self.__lock:
                self.__file_storage.destroy()
                self.__file_storage.create()

                with zipfile.ZipFile(filename, 'r') as f:
                    for member in f.namelist():
                        if member != 'raft.bin':
                            f.extract(member, path=self.__file_storage.folder)
                    return pickle.loads(f.read('raft.bin'))
        except Exception as ex:
            self.__logger.error(ex)

    def get_file_storage(self):
        return self.__file_storage

    def __index_exists(self, index_name):
        return self.__file_storage.index_exists(indexname=index_name)

    def __get_existing_index_names(self):
        index_names = []

        pattern_toc = re.compile('^_(.+)_\\d+\\.toc$')
        for filename in self.__file_storage:
            match = pattern_toc.search(filename)
            if match:
                index_names.append(match.group(1))

        return index_names

    def __open_existing_indices(self):
        for index_name in self.__get_existing_index_names():
            self.open_index(index_name, schema=None, _doApply=False)

    @replicated
    def open_index(self, index_name, schema=None):
        index = None

        try:
            if index_name in self.__indices:
                index = self.__indices[index_name]
            else:
                self.__indices[index_name] = self.__file_storage.open_index(indexname=index_name, schema=schema)
                index = self.__indices[index_name]
        except Exception as ex:
            self.__logger.info(
                'failed to open {0} in file storage on {1}: {2}'.format(index_name, self.__file_storage.folder, ex))

        return index

    @replicated
    def close_index(self, index_name):
        index = None

        try:
            index = self.__indices.pop(index_name)
            index.close()
        except KeyError as ex:
            self.__logger.error('{0} does not exist'.format(ex.args[0]))
        except Exception as ex:
            self.__logger.error('failed to close {0}: {1}'.format(index_name, ex))

        return index

    @replicated
    def create_index(self, index_name, schema):
        index = None

        try:
            if self.__index_exists(index_name):
                self.open_index(index_name, schema=schema, _doApply=True)
            else:
                self.__indices[index_name] = self.__file_storage.create_index(schema, indexname=index_name)
                index = self.__indices[index_name]
        except Exception as ex:
            self.__logger.error(
                'failed to close {0} in file storage on {1}: {2}'.format(index_name, self.__file_storage.folder, ex))

        return index

    @replicated
    def delete_index(self, index_name):
        # close index
        index = self.close_index(index_name, _doApply=True)

        # delete index files
        try:
            pattern_toc = re.compile('^_{0}_(\\d+)\\.toc$'.format(index_name))
            for filename in self.__file_storage:
                if re.match(pattern_toc, filename):
                    self.__file_storage.delete_file(filename)
            pattern_seg = re.compile('^{0}_([a-z0-9]+)\\.seg$'.format(index_name))
            for filename in self.__file_storage:
                if re.match(pattern_seg, filename):
                    self.__file_storage.delete_file(filename)
            pattern_lock = re.compile('^{0}_WRITELOCK$'.format(index_name))
            for filename in self.__file_storage:
                if re.match(pattern_lock, filename):
                    self.__file_storage.delete_file(filename)
        except Exception as ex:
            self.__logger.error(
                'failed to delete {0} in file storage on {1}: {2}'.format(index_name, self.__file_storage.folder, ex))

        return index

    def get_indices(self):
        return self.__indices

    def get_index(self, index_name):

        try:
            index = self.__indices[index_name]
        except KeyError as ex:
            raise KeyError('{0} does not exist'.format(ex.args[0]))
        except Exception as ex:
            raise ex

        return index

    @replicated
    def optimize_index(self, index_name):
        index = None

        try:
            index = self.get_index(index_name)
            index.optimize()
        except Exception as ex:
            self.__logger.error('failed to optimize {0}: {1}'.format(index_name, ex))

        return index

    def get_doc_count(self, index_name):
        try:
            cnt = self.get_index(index_name).doc_count()
        except Exception as ex:
            raise ex

        return cnt

    def get_writer(self, index_name):
        try:
            writer = self.get_index(index_name).writer()
        except Exception as ex:
            raise ex

        return writer

    def get_schema(self, index_name):
        try:
            schema = self.get_index(index_name).schema
        except Exception as ex:
            raise ex

        return schema

    def get_searcher(self, index_name, weighting=None):
        try:
            if weighting is None:
                searcher = self.get_index(index_name).searcher()
            else:
                searcher = self.get_index(index_name).searcher(weighting=weighting)
        except Exception as ex:
            raise ex

        return searcher

    @replicated
    def put_document(self, index_name, doc_id, fields):
        count = 0
        success = False
        writer = None

        try:
            tmp_fields = copy.deepcopy(fields)
            tmp_fields[self.get_schema(index_name).get_doc_id_field()] = doc_id

            writer = self.get_writer(index_name)
            writer.update_document(**tmp_fields)
            count += 1
            success = True
        except Exception as ex:
            count = -1
            self.__logger.error('failed to index document in {0}: {1}'.format(index_name, ex))
        finally:
            if writer is not None:
                if success and count > 0:
                    writer.commit()
                else:
                    writer.cancel()

        return count

    def get_document(self, index_name, doc_id):
        try:
            results_page = self.search_documents(index_name, doc_id, self.get_schema(index_name).get_doc_id_field(), 1,
                                                 page_len=1)
        except Exception as ex:
            raise ex

        return results_page

    @replicated
    def delete_document(self, index_name, doc_id):
        count = 0
        success = False
        writer = None

        try:
            writer = self.get_writer(index_name)
            count = writer.delete_by_term(self.get_schema(index_name).get_doc_id_field(), doc_id)
            success = True
        except Exception as ex:
            count = -1
            self.__logger.error('failed to delete document in {0}: {1}'.format(index_name, ex))
        finally:
            if writer is not None:
                if success and count > 0:
                    writer.commit()
                else:
                    writer.cancel()

        return count

    @replicated
    def put_documents(self, index_name, docs):
        count = 0
        success = False
        writer = None

        try:
            writer = self.get_writer(index_name)
            for doc in docs:
                writer.update_document(**doc)
                count = count + 1
            success = True
        except Exception as ex:
            count = -1
            self.__logger.error('failed to index documents in {0} in bulk: {1}'.format(index_name, ex))
        finally:
            if writer is not None:
                if success and count > 0:
                    writer.commit()
                else:
                    writer.cancel()

        return count

    @replicated
    def delete_documents(self, index_name, doc_ids):
        count = 0
        success = False
        writer = None

        try:
            writer = self.get_writer(index_name)
            for doc_id in doc_ids:
                count = count + writer.delete_by_term(self.get_schema(index_name).get_doc_id_field(), doc_id)
            success = True
        except Exception as ex:
            count = -1
            self.__logger.error('failed to delete documents in {0} in bulk: {1}'.format(index_name, ex))
        finally:
            if writer is not None:
                if success and count > 0:
                    writer.commit()
                else:
                    writer.cancel()

        return count

    def search_documents(self, index_name, query, search_field, page_num, page_len=10, weighting=None, **kwargs):
        try:
            searcher = self.get_searcher(index_name, weighting=weighting)
            query_parser = QueryParser(search_field, self.get_schema(index_name))
            query_obj = query_parser.parse(query)
            results_page = searcher.search_page(query_obj, page_num, pagelen=page_len, **kwargs)
        except Exception as ex:
            raise ex

        return results_page

    def get_snapshot_file_name(self):
        return self.__conf.fullDumpFile

    def snapshot_exists(self):
        return os.path.exists(self.get_snapshot_file_name())

    def open_snapshot_file(self):
        try:
            file = open(self.get_snapshot_file_name(), mode='rb')
        except Exception as ex:
            raise ex

        return file
