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

from logging import getLogger

from pysyncobj import SyncObj, replicated
from whoosh.filedb.filestore import FileStorage, RamStorage
from whoosh.qparser import QueryParser

from cockatrice import NAME


class IndexServer(SyncObj):
    def __init__(self, bind_addr, peer_addrs=None, conf=None, index_dir=None, logger=getLogger(NAME)):
        self.__logger = logger
        self.__logger.info('starting index server')

        peer_addrs = peer_addrs if peer_addrs is not None else []

        self.__logger.info('binding an address of the transport on {0}'.format(bind_addr))
        self.__logger.info('setting peer addresses is {0}'.format(peer_addrs))
        super(IndexServer, self).__init__(bind_addr, peer_addrs, conf=conf)

        self.__indices = {}

        self.__logger.info('creating ram storage on memory')
        self.__ram_storage = RamStorage()

        self.__file_storage = None
        if index_dir is None:
            self.__logger.info('skipping file storage creation')
        else:
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

    def get_indices(self):
        return self.__indices

    def get_ram_storage(self):
        return self.__ram_storage

    def get_file_storage(self):
        return self.__file_storage

    def __index_exists(self, index_name):
        return self.__ram_storage.index_exists(indexname=index_name) \
               or (self.__file_storage is not None and self.__file_storage.index_exists(indexname=index_name))

    def __get_existing_index_names(self):
        index_names = []

        self.__logger.info('seeking indices on memory')
        pattern_toc = re.compile('^_(.+)_\\d+\\.toc$')
        for filename in self.__ram_storage:
            match = pattern_toc.search(filename)
            if match:
                index_names.append(match.group(1))
        if self.__file_storage is not None:
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
            if self.__ram_storage.index_exists(indexname=index_name):
                self.__logger.info('opening {0} in ram storage on memory'.format(index_name))
                self.__indices[index_name] = self.__ram_storage.open_index(indexname=index_name, schema=schema)
            elif self.__file_storage is not None and self.__file_storage.index_exists(indexname=index_name):
                self.__logger.info('opening {0} in file storage on {1}'.format(index_name, self.__file_storage.folder))
                self.__indices[index_name] = self.__file_storage.open_index(indexname=index_name, schema=schema)
            else:
                raise KeyError('index does not exist')
            index = self.__indices[index_name]
        except Exception as ex:
            self.__logger.error(ex)

        return index

    @replicated
    def create_index(self, index_name, schema, use_ram_storage=False):
        index = None
        try:
            if self.__index_exists(index_name):
                raise KeyError('index already exists')

            if use_ram_storage:
                self.__logger.info('creating {0} in ram storage on memory'.format(index_name))
                self.__indices[index_name] = self.__ram_storage.create_index(schema, indexname=index_name)
            else:
                if self.__file_storage is not None:
                    self.__logger.info(
                        'creating {0} in file storage on {1}'.format(index_name, self.__file_storage.folder))
                    self.__indices[index_name] = self.__file_storage.create_index(schema, indexname=index_name)
                else:
                    raise ValueError('file storage has not been created')
            index = self.__indices[index_name]
        except Exception as ex:
            self.__logger.error(ex)

        return index

    def get_index(self, index_name):
        index = self.__indices[index_name] if index_name in self.__indices else None
        if index is None:
            self.__logger.info('index is not available')

        return index

    @replicated
    def optimize_index(self, index_name):
        index = self.get_index(index_name)
        if index is not None:
            if index.storage.folder == '':
                self.__logger.info('optimizing {0} in ram storage on memory'.format(index_name))
            else:
                self.__logger.info('optimizing {0} in file storage on {1}'.format(index_name, index.storage.folder))
            index.optimize()

    def get_doc_count(self, index_name):
        index = self.get_index(index_name)

        return None if index is None else index.doc_count()

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
    def delete_index(self, index_name):
        # close index
        self.close_index(index_name, _doApply=True)

        # delete index files
        try:
            storage = None
            if self.__ram_storage.index_exists(indexname=index_name):
                storage = self.__ram_storage
            elif self.__file_storage is not None and self.__file_storage.index_exists(indexname=index_name):
                storage = self.__file_storage
            else:
                raise KeyError('index does not exist')

            if storage.folder == '':
                self.__logger.info('deleting {0} in ram storage on memory'.format(index_name))
            else:
                self.__logger.info('deleting {0} in file storage on {1}'.format(index_name, storage.folder))

            pattern_toc = re.compile('^_{0}_(\\d+)\\.toc$'.format(index_name))
            for filename in storage:
                if re.match(pattern_toc, filename):
                    self.__logger.info('deleting {0}'.format(os.path.join(storage.folder, filename)))
                    storage.delete_file(filename)
            pattern_seg = re.compile('^{0}_([a-z0-9]+)\\.seg$'.format(index_name))
            for filename in storage:
                if re.match(pattern_seg, filename):
                    self.__logger.info('deleting {0}'.format(os.path.join(storage.folder, filename)))
                    storage.delete_file(filename)
            pattern_lock = re.compile('^{0}_WRITELOCK$'.format(index_name))
            for filename in storage:
                if re.match(pattern_lock, filename):
                    self.__logger.info('deleting {0}'.format(os.path.join(storage.folder, filename)))
                    storage.delete_file(filename)
        except Exception as ex:
            self.__logger.error(ex)

    @replicated
    def index_document(self, index_name, doc_id, fields):
        writer = self.__indices[index_name].writer()

        try:
            doc = {self.__indices[index_name].schema.get_unique_field(): doc_id}
            doc.update(fields)

            self.__logger.debug('indexing document in {0}: {1}'.format(index_name, doc))

            writer.update_document(**doc)
            writer.commit()
        except Exception as ex:
            self.__logger.error(ex)
            writer.cancel()

    @replicated
    def delete_document(self, index_name, doc_id):
        writer = self.__indices[index_name].writer()

        try:
            unique_field = self.__indices[index_name].schema.get_unique_field()

            self.__logger.debug('deleting document in {0}: {1}:{2}'.format(index_name, unique_field, doc_id))

            writer.delete_by_term(unique_field, doc_id)
            writer.commit()
        except Exception as ex:
            self.__logger.error(ex)
            writer.cancel()

    @replicated
    def index_documents(self, index_name, docs):
        writer = self.__indices[index_name].writer()

        cnt = 0
        try:
            for doc in docs:
                self.__logger.debug('indexing document in {0}: {1}'.format(index_name, doc))

                writer.update_document(**doc)
                cnt = cnt + 1

            writer.commit()
        except Exception as ex:
            self.__logger.error(ex)
            writer.cancel()
            cnt = 0

        return cnt

    @replicated
    def delete_documents(self, index_name, doc_ids):
        writer = self.__indices[index_name].writer()

        cnt = 0
        try:
            unique_field = self.__indices[index_name].schema.get_unique_field()

            for doc_id in doc_ids:
                self.__logger.debug('deleting document in {0}: {1}:{2}'.format(index_name, unique_field, doc_id))

                writer.delete_by_term(unique_field, doc_id)
                cnt = cnt + 1

            writer.commit()
        except Exception as ex:
            self.__logger.error(ex)
            writer.cancel()
            cnt = 0

        return cnt

    def get_document(self, index_name, doc_id):
        doc = None
        try:
            unique_field = self.__indices[index_name].schema.get_unique_field()

            self.__logger.debug('getting document in {0}: {1}:{2}'.format(index_name, unique_field, doc_id))

            doc = self.search_documents(index_name, doc_id, unique_field, 1, 1)
        except Exception as ex:
            self.__logger.error(ex)

        return doc

    def search_documents(self, index_name, query, search_field, page_num, page_len=10, **kwargs):
        searcher = self.__indices[index_name].searcher()

        results_page = None
        try:
            query_parser = QueryParser(search_field, self.__indices[index_name].schema)
            query = query_parser.parse(query)

            self.__logger.debug('searching documents in {0}: {1}'.format(index_name, query))

            results_page = searcher.search_page(query, page_num, pagelen=page_len, **kwargs)
        except Exception as ex:
            self.__logger.error(ex)

        return results_page
