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
from whoosh.index import create_in, exists_in, open_dir
from whoosh.qparser import QueryParser


class DataNode(SyncObj):
    def __init__(self, addr, peer_addrs, conf, index_dir, schema, logger=getLogger(__name__)):
        super(DataNode, self).__init__(addr, peer_addrs, conf=conf)

        self.__logger = logger

        self.__schema = schema

        if exists_in(index_dir):
            self.__index = open_dir(index_dir)
        else:
            if index_dir is not None:
                os.makedirs(index_dir, exist_ok=True)
            self.__index = create_in(index_dir, self.__schema)

    @replicated
    def index(self, doc_id, fields):
        writer = self.__index.writer()

        try:
            doc = {self.__schema.get_unique_field(): doc_id}
            doc.update(fields)

            writer.update_document(**doc)
            writer.commit()
        except Exception as ex:
            self.__logger.error(ex)
            writer.cancel()

    @replicated
    def delete(self, doc_id):
        writer = self.__index.writer()

        try:
            writer.delete_by_term(self.__schema.get_unique_field(), doc_id)
            writer.commit()
        except Exception as ex:
            self.__logger.error(ex)
            writer.cancel()

    @replicated
    def bulk_index(self, docs):
        writer = self.__index.writer()

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
    def bulk_delete(self, doc_ids):
        writer = self.__index.writer()

        cnt = 0
        try:
            for doc_id in doc_ids:
                writer.delete_by_term(self.__schema.get_unique_field(), doc_id)
                cnt = cnt + 1

            writer.commit()
        except Exception as ex:
            self.__logger.error(ex)
            writer.cancel()
            cnt = 0

        return cnt

    def get(self, doc_id):
        return self.search(doc_id, self.__schema.get_unique_field(), 1, 1)

    def search(self, query, search_field, page_num, page_len=10, **kwargs):
        searcher = self.__index.searcher()

        query_parser = QueryParser(search_field, self.__schema)
        query = query_parser.parse(query)

        results_page = searcher.search_page(query, page_num, pagelen=page_len, **kwargs)

        return results_page
