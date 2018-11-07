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
    def __init__(self, bind_addr, peer_addrs, conf=None, index_dir='/tmp/cockatrice/index', logger=getLogger(NAME)):
        super(IndexServer, self).__init__(bind_addr, peer_addrs, conf=conf)

        self.__logger = logger

        self.__bind_addr = bind_addr

        self.__index_dir = index_dir
        os.makedirs(self.__index_dir, exist_ok=True)

        self.__indices = {}
        self.__ram_storage = RamStorage()
        self.__file_storage = FileStorage(self.__index_dir, supports_mmap=True, readonly=False, debug=False)

    def destroy(self):
        for index in self.__indices.values():
            index.close()

        super().destroy()

    def get_indices(self):
        return self.__indices

    def get_ram_storage(self):
        return self.__ram_storage

    def get_file_storage(self):
        return self.__file_storage

    def get_bind_addr(self):
        return self.__bind_addr

    @replicated
    def create_index(self, index_name, schema, use_ram_storage=False):
        index = None

        try:
            if use_ram_storage:
                if self.__ram_storage.index_exists(indexname=index_name):
                    self.__indices[index_name] = self.__ram_storage.open_index(indexname=index_name, schema=schema)
                    self.__logger.info('open {0} on memory'.format(index_name))
                else:
                    self.__indices[index_name] = self.__ram_storage.create_index(schema, indexname=index_name)
                    self.__logger.info('create {0} on memory'.format(index_name))
            else:
                if self.__file_storage.index_exists(indexname=index_name):
                    self.__indices[index_name] = self.__file_storage.open_index(indexname=index_name, schema=schema)
                    self.__logger.info('open {0} on {1}'.format(index_name, self.__index_dir))
                else:
                    self.__indices[index_name] = self.__file_storage.create_index(schema, indexname=index_name)
                    self.__logger.info('create {0} on {1}'.format(index_name, self.__index_dir))

            index = self.__indices[index_name]
        except Exception as ex:
            self.__logger.error(ex)

        return index

    @replicated
    def delete_index(self, index_name):
        try:
            index = self.__indices.pop(index_name, None)
            if index is not None:
                # close index
                index.close()
                self.__logger.info('close {0} on {1}'.format(index_name, self.__index_dir))

                # delete files
                pattern_toc = re.compile('^_{0}_(\\d+)\\.toc$'.format(index_name))
                for filename in index.storage:
                    if re.match(pattern_toc, filename):
                        index.storage.delete_file(filename)
                        self.__logger.info(
                            'delete {0} on {1}'.format(index_name, os.path.join(self.__index_dir, filename)))
                pattern_seg = re.compile('^{0}_([a-z0-9]+)\\.seg$'.format(index_name))
                for filename in index.storage:
                    if re.match(pattern_seg, filename):
                        index.storage.delete_file(filename)
                        self.__logger.info(
                            'delete {0} on {1}'.format(index_name, os.path.join(self.__index_dir, filename)))
                pattern_lock = re.compile('^{0}_WRITELOCK$'.format(index_name))
                for filename in index.storage:
                    if re.match(pattern_lock, filename):
                        index.storage.delete_file(filename)
                        self.__logger.info(
                            'delete {0} on {1}'.format(index_name, os.path.join(self.__index_dir, filename)))
        except Exception as ex:
            self.__logger.error(ex)

    def index_exists(self, index_name):
        if index_name in self.__indices:
            return True
        else:
            return False

    def get_index(self, index_name):
        if self.index_exists(index_name):
            return self.__indices[index_name]
        else:
            return None

    def get_doc_count(self, index_name):
        index = self.get_index(index_name)
        if index is not None:
            return index.doc_count()
        else:
            return None

    @replicated
    def index_document(self, index_name, doc_id, fields):
        writer = self.__indices[index_name].writer()

        try:
            doc = {self.__indices[index_name].schema.get_unique_field(): doc_id}
            doc.update(fields)

            writer.update_document(**doc)
            writer.commit()
        except Exception as ex:
            self.__logger.error(ex)
            writer.cancel()

    @replicated
    def delete_document(self, index_name, doc_id):
        writer = self.__indices[index_name].writer()

        try:
            writer.delete_by_term(self.__indices[index_name].schema.get_unique_field(), doc_id)
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
            for doc_id in doc_ids:
                writer.delete_by_term(self.__indices[index_name].schema.get_unique_field(), doc_id)
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
            doc = self.search_documents(index_name, doc_id, self.__indices[index_name].schema.get_unique_field(), 1, 1)
        except Exception as ex:
            self.__logger.error(ex)

        return doc

    def search_documents(self, index_name, query, search_field, page_num, page_len=10, **kwargs):
        searcher = self.__indices[index_name].searcher()

        results_page = None
        try:
            query_parser = QueryParser(search_field, self.__indices[index_name].schema)
            query = query_parser.parse(query)

            results_page = searcher.search_page(query, page_num, pagelen=page_len, **kwargs)
        except Exception as ex:
            self.__logger.error(ex)

        return results_page
