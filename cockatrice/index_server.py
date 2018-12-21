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

import os
import re
import threading
import zipfile
import pysyncobj.pickle as pickle

from pysyncobj import SyncObj, replicated
from whoosh.filedb.filestore import FileStorage
from whoosh.qparser import QueryParser

import cockatrice


class IndexServer(SyncObj):
    def __init__(self, bind_addr, peer_addrs=cockatrice.DEFAULT_PEER_ADDRS, conf=cockatrice.DEFAULT_SYNC_CONFIG, index_dir=cockatrice.DEFAULT_INDEX_DIR, logger=cockatrice.DEFAULT_LOGGER):
        self.__logger = logger
        self.__logger.info('starting index server: {0}, {1}'.format(bind_addr, peer_addrs))

        self.__lock = threading.RLock()

        conf.serializer = self.__serialize
        conf.deserializer = self.__deserialize
        conf.validate()

        super(IndexServer, self).__init__(bind_addr, peer_addrs, conf=conf)

        self.__indices = {}

        self.__logger.info('creating index dir on {0}'.format(index_dir))
        os.makedirs(index_dir, exist_ok=True)
        self.__logger.info('creating file storage on {0}'.format(index_dir))
        self.__file_storage = FileStorage(index_dir, supports_mmap=True, readonly=False, debug=False)

        # open existing indices on startup
        self.__open_existing_indices()

    def destroy(self):
        self.__logger.info('stopping index server')

        for index_name in self.__indices.keys():
            self.close_index(index_name)

        super().destroy()

    def __serialize(self, filename, raft_data):
        try:
            with self.__lock:
                self.__logger.info('creating {0}'.format(filename))
                with zipfile.ZipFile(filename, 'w', zipfile.ZIP_DEFLATED) as f:
                    for index_filename in self.__file_storage:
                        abs_index_path = os.path.join(self.__file_storage.folder, index_filename)
                        self.__logger.info('storing {0}'.format(abs_index_path))
                        f.write(abs_index_path, index_filename)
                    self.__logger.info('storing Raft data')
                    f.writestr('raft.bin', pickle.dumps(raft_data))
        except Exception as ex:
            self.__logger.error(ex)

    def __deserialize(self, filename):
        try:
            with self.__lock:
                self.__logger.info('cleaning {0}'.format(self.__file_storage.folder))
                self.__file_storage.destroy()

                self.__logger.info('reading {0}'.format(filename))
                with zipfile.ZipFile(filename, 'r') as f:
                    for member in f.namelist():
                        if member != 'raft.bin':
                            self.__logger.info('restoring {0}'.format(os.path.join(self.__file_storage.folder, member)))
                            f.extract(member, path=self.__file_storage.folder)
                    self.__logger.info('restoring Raft data')
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
        self.__logger.info('seeking indices on {0}'.format(self.__file_storage.folder))
        for filename in self.__file_storage:
            match = pattern_toc.search(filename)
            if match:
                index_names.append(match.group(1))

        return index_names

    def __open_existing_indices(self):
        for index_name in self.__get_existing_index_names():
            self.open_index(index_name, schema=None)

    def open_index(self, index_name, schema=None):
        index = None

        try:
            if self.__file_storage.index_exists(indexname=index_name):
                self.__logger.info('opening {0} in file storage on {1}'.format(index_name, self.__file_storage.folder))
                index = self.__file_storage.open_index(indexname=index_name, schema=schema)
            else:
                raise KeyError('index does not exist')
            self.__indices[index_name] = index
        except Exception as ex:
            self.__logger.error('failed to open {0}: {1}'.format(index_name, ex))

        return index

    @replicated
    def close_index(self, index_name):
        index = self.__indices.pop(index_name)

        if index is None:
            self.__logger.info('{0} already closed'.format(index_name))
        else:
            if index.storage.folder == '':
                self.__logger.info('closing {0} in ram storage on memory'.format(index_name))
            else:
                self.__logger.info('closing {0} in file storage on {1}'.format(index_name, index.storage.folder))
            index.close()

        return index

    @replicated
    def create_index(self, index_name, schema):
        index = None

        try:
            if self.__index_exists(index_name):
                raise KeyError('index already exists')

            self.__logger.info(
                'creating {0} in file storage on {1}'.format(index_name, self.__file_storage.folder))
            self.__indices[index_name] = self.__file_storage.create_index(schema, indexname=index_name)
            index = self.__indices[index_name]
        except Exception as ex:
            self.__logger.error('failed to create {0}: {1}'.format(index_name, ex))

        return index

    @replicated
    def delete_index(self, index_name):
        success = False

        # close index
        self.close_index(index_name, _doApply=True)

        # delete index files
        try:
            self.__logger.info('deleting {0} in file storage on {1}'.format(index_name, self.__file_storage.folder))

            pattern_toc = re.compile('^_{0}_(\\d+)\\.toc$'.format(index_name))
            for filename in self.__file_storage:
                if re.match(pattern_toc, filename):
                    self.__logger.info('deleting {0}'.format(os.path.join(self.__file_storage.folder, filename)))
                    self.__file_storage.delete_file(filename)
            pattern_seg = re.compile('^{0}_([a-z0-9]+)\\.seg$'.format(index_name))
            for filename in self.__file_storage:
                if re.match(pattern_seg, filename):
                    self.__logger.info('deleting {0}'.format(os.path.join(self.__file_storage.folder, filename)))
                    self.__file_storage.delete_file(filename)
            pattern_lock = re.compile('^{0}_WRITELOCK$'.format(index_name))
            for filename in self.__file_storage:
                if re.match(pattern_lock, filename):
                    self.__logger.info('deleting {0}'.format(os.path.join(self.__file_storage.folder, filename)))
                    self.__file_storage.delete_file(filename)

            success = True
        except Exception as ex:
            self.__logger.error('failed to delete {0}: {1}'.format(index_name, ex))

        return success

    def get_indices(self):
        return self.__indices

    def get_index(self, index_name):
        index = self.get_indices().get(index_name)
        if index is None:
            msg = '{0} is not available'.format(index_name)
            self.__logger.error(msg)
            raise KeyError(msg)
        return index

    @replicated
    def optimize_index(self, index_name):
        success = False

        try:
            index = self.get_index(index_name)
            if index.storage.folder == '':
                self.__logger.info('optimizing {0} in ram storage on memory'.format(index_name))
            else:
                self.__logger.info('optimizing {0} in file storage on {1}'.format(index_name, index.storage.folder))
            index.optimize()
            success = True
        except Exception as ex:
            self.__logger.error('failed to optimize {0}: {1}'.format(index_name, ex))

        return success

    def get_doc_count(self, index_name):
        try:
            cnt = self.get_index(index_name).doc_count()
        except Exception as ex:
            self.__logger.error('failed to get document count in {0}'.format(index_name))
            raise ex

        return cnt

    def get_writer(self, index_name):
        try:
            writer = self.get_index(index_name).writer()
        except Exception as ex:
            self.__logger.error('failed to get index writer in {0}'.format(index_name))
            raise ex

        return writer

    def get_schema(self, index_name):
        try:
            schema = self.get_index(index_name).schema
        except Exception as ex:
            self.__logger.error('failed to get index schema in {0}'.format(index_name))
            raise ex

        return schema

    def get_searcher(self, index_name, weighting=None):
        try:
            if weighting is None:
                searcher = self.get_index(index_name).searcher()
            else:
                searcher = self.get_index(index_name).searcher(weighting=weighting)
        except Exception as ex:
            self.__logger.error('failed to get index searcher in {0}'.format(index_name))
            raise ex

        return searcher

    @replicated
    def put_document(self, index_name, doc_id, fields):
        success = False

        try:
            writer = self.get_writer(index_name)
            doc = {
                self.get_schema(index_name).get_unique_field(): doc_id
            }
            doc.update(fields)
            try:
                self.__logger.debug('indexing document in {0}: {1}'.format(index_name, doc))
                writer.update_document(**doc)
                success = True
            except Exception as ex:
                self.__logger.error('failed to index document in {0}: {1}'.format(index_name, doc))
                raise ex
            finally:
                if success:
                    writer.commit()
                else:
                    writer.cancel()
        except Exception as ex:
            self.__logger.error('failed to index document in {0}: {1}'.format(index_name, ex))

        return success

    @replicated
    def delete_document(self, index_name, doc_id):
        success = False

        try:
            writer = self.get_writer(index_name)
            unique_field = self.get_schema(index_name).get_unique_field()
            try:
                self.__logger.debug('deleting document in {0}: {1}:{2}'.format(index_name, unique_field, doc_id))
                writer.delete_by_term(unique_field, doc_id)
                success = True
            except Exception as ex:
                self.__logger.error('failed to delete document in {0}: {1}:{2}'.format(index_name, unique_field, doc_id))
                raise ex
            finally:
                if success:
                    writer.commit()
                else:
                    writer.cancel()
        except Exception as ex:
            self.__logger.error('failed to delete document in {0}: {1}'.format(index_name, ex))

        return success

    @replicated
    def put_documents(self, index_name, docs):
        cnt = 0

        try:
            success = False
            writer = self.get_writer(index_name)
            try:
                for doc in docs:
                    self.__logger.debug('indexing document in {0}: {1}'.format(index_name, doc))
                    writer.update_document(**doc)
                    cnt = cnt + 1
                success = True
            except Exception as ex:
                self.__logger.error('failed to index documents in {0} in bulk'.format(index_name))
                raise ex
            finally:
                if success:
                    writer.commit()
                else:
                    cnt = 0  # clear
                    writer.cancel()
        except Exception as ex:
            self.__logger.error('failed to index documents in {0} in bulk: {1}'.format(index_name, ex))

        return cnt

    @replicated
    def delete_documents(self, index_name, doc_ids):
        cnt = 0

        try:
            success = False
            writer = self.get_writer(index_name)
            unique_field = self.get_schema(index_name).get_unique_field()
            try:
                for doc_id in doc_ids:
                    self.__logger.debug('deleting document in {0}: {1}:{2}'.format(index_name, unique_field, doc_id))
                    writer.delete_by_term(unique_field, doc_id)
                    cnt = cnt + 1
                success = True
            except Exception as ex:
                self.__logger.error('failed to delete documents in {0} in bulk'.format(index_name))
                raise ex
            finally:
                if success:
                    writer.commit()
                else:
                    cnt = 0  # clear
                    writer.cancel()
        except Exception as ex:
            self.__logger.error('failed to delete documents in {0} in bulk: {1}'.format(index_name, ex))

        return cnt

    def get_document(self, index_name, doc_id):
        try:
            unique_field = self.get_schema(index_name).get_unique_field()
            try:
                self.__logger.debug('getting document in {0}: {1}:{2}'.format(index_name, unique_field, doc_id))
                doc = self.search_documents(index_name, doc_id, unique_field, 1, page_len=1)
            except Exception as ex:
                self.__logger.error('failed to get document in {0}: {1}:{2}'.format(index_name, unique_field, doc_id))
                raise ex
        except Exception as ex:
            self.__logger.error('failed to get document in {0}'.format(index_name))
            raise ex

        return doc

    def search_documents(self, index_name, query, search_field, page_num, page_len=10, weighting=None, **kwargs):
        try:
            searcher = self.get_searcher(index_name, weighting=weighting)
            query_parser = QueryParser(search_field, self.get_schema(index_name))
            query_obj = query_parser.parse(query)
            try:
                self.__logger.debug('searching documents in {0}: {1}'.format(index_name, query_obj))
                results_page = searcher.search_page(query_obj, page_num, pagelen=page_len, **kwargs)
            except Exception as ex:
                self.__logger.error('failed to search documents in {0}: {1}'.format(index_name, query_obj))
                raise ex
        except Exception as ex:
            self.__logger.error('failed to search documents in {0}'.format(index_name))
            raise ex

        return results_page
