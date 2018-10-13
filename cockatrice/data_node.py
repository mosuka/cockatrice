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

from logging import getLogger

from pysyncobj import SyncObj, replicated
from whoosh.filedb.filestore import FileStorage, RamStorage
from whoosh.qparser import QueryParser


class DataNode(SyncObj):
    def __init__(self, bind_addr, peer_addrs, conf, index_dir, logger=getLogger(__name__)):
        super(DataNode, self).__init__(bind_addr, peer_addrs, conf=conf)

        self.__logger = logger

        self.index_dir = index_dir
        os.makedirs(self.index_dir, exist_ok=True)

        self.indices = {}
        self.ram_storage = RamStorage()
        self.file_storage = FileStorage(self.index_dir, supports_mmap=True, readonly=False, debug=False)

    def destroy(self):
        for index in self.indices.values():
            index.close()

        super().destroy()

    @replicated
    def create_index(self, index_name, schema, use_ram_storage=False):
        if use_ram_storage:
            if self.ram_storage.index_exists(indexname=index_name):
                self.indices[index_name] = self.ram_storage.open_index(indexname=index_name, schema=schema)
                self.__logger.info('open {0} on memory'.format(index_name))
            else:
                self.indices[index_name] = self.ram_storage.create_index(schema, indexname=index_name)
                self.__logger.info('create {0} on memory'.format(index_name))
        else:
            if self.file_storage.index_exists(indexname=index_name):
                self.indices[index_name] = self.file_storage.open_index(indexname=index_name, schema=schema)
                self.__logger.info('open {0} on {1}'.format(index_name, self.index_dir))
            else:
                self.indices[index_name] = self.file_storage.create_index(schema, indexname=index_name)
                self.__logger.info('create {0} on {1}'.format(index_name, self.index_dir))
        return self.indices[index_name]

    @replicated
    def delete_index(self, index_name):
        __index = self.indices.pop(index_name, None)
        if __index is not None:
            # # clear index
            # __index.writer().commit(mergetype=CLEAR)
            # self.__logger.info('delete {0} on {1}'.format(index_name, self.index_dir))

            # close index
            __index.close()
            self.__logger.info('close {0} on {1}'.format(index_name, self.index_dir))

            # delete files
            prefix = "_%s_" % index_name
            for filename in __index.storage:
                if filename.startswith(prefix):
                    __index.storage.delete_file(filename)
                    self.__logger.info('delete {0} on {1}'.format(index_name, os.path.join(self.index_dir, filename)))

    def index_exists(self, index_name):
        if index_name in self.indices:
            return True
        else:
            return False

    def get_index(self, index_name):
        return self.indices[index_name]

    @replicated
    def index_document(self, index_name, doc_id, fields):
        writer = self.indices[index_name].writer()

        try:
            doc = {self.indices[index_name].schema.get_unique_field(): doc_id}
            doc.update(fields)

            writer.update_document(**doc)
            writer.commit()
        except Exception as ex:
            self.__logger.error(ex)
            writer.cancel()

    @replicated
    def delete_document(self, index_name, doc_id):
        writer = self.indices[index_name].writer()

        try:
            writer.delete_by_term(self.indices[index_name].schema.get_unique_field(), doc_id)
            writer.commit()
        except Exception as ex:
            self.__logger.error(ex)
            writer.cancel()

    @replicated
    def index_documents(self, index_name, docs):
        writer = self.indices[index_name].writer()

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
        writer = self.indices[index_name].writer()

        cnt = 0
        try:
            for doc_id in doc_ids:
                writer.delete_by_term(self.indices[index_name].schema.get_unique_field(), doc_id)
                cnt = cnt + 1

            writer.commit()
        except Exception as ex:
            self.__logger.error(ex)
            writer.cancel()
            cnt = 0

        return cnt

    def get_document(self, index_name, doc_id):
        return self.search_documents(index_name, doc_id, self.indices[index_name].schema.get_unique_field(), 1, 1)

    def search_documents(self, index_name, query, search_field, page_num, page_len=10, **kwargs):
        searcher = self.indices[index_name].searcher()

        query_parser = QueryParser(search_field, self.indices[index_name].schema)
        query = query_parser.parse(query)

        results_page = searcher.search_page(query, page_num, pagelen=page_len, **kwargs)

        return results_page
