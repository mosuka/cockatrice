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

import threading
from logging import getLogger

from whoosh.writing import IndexWriter as WhooshIndexWriter


class IndexWriter(WhooshIndexWriter):
    def __init__(self, index, procs=None, batchsize=100, subargs=None, multisegment=False, period=60, limit=10,
                 logger=getLogger(), **kwargs):

        self.__index = index
        self.__procs = procs
        self.__batchsize = batchsize
        self.__subargs = subargs
        self.__multisegment = multisegment
        self.__period = period
        self.__limit = limit
        self.__logger = logger
        self.__kwargs = kwargs

        self.__writer = self.__index.writer(proc=self.__procs, batchsize=self.__batchsize, subargs=self.__subargs,
                                            multisegment=self.__multisegment, **self.__kwargs)

        self.__limit_counter = 0

        self.__lock = threading.Lock()

        if self.__period:
            self.autocommit_timer = threading.Timer(self.__period, self.commit)
            self.autocommit_timer.start()

    def reader(self, **kwargs):
        return self.__writer.reader(**kwargs)

    def delete_document(self, docnum, delete=True):
        self.__writer.delete_document(docnum, delete=delete)

    def add_document(self, **fields):
        self.__writer.add_document(**fields)

    def update_document(self, **fields):
        with self.lock():
            self.__writer.update_document(**fields)

    def add_reader(self, reader):
        self.__writer.add_reader(reader)

    def commit(self, mergetype=None, optimize=None, merge=None, restart=True):
        with self.lock():
            if self.__period:
                self.autocommit_timer.cancel()

            if not self.is_closed():
                self.__writer.commit(mergetype=mergetype, optimize=optimize, merge=merge)

                if restart:
                    self.__writer = self.__index.writer(proc=self.__procs, batchsize=self.__batchsize,
                                                        subargs=self.__subargs, multisegment=self.__multisegment,
                                                        **self.__kwargs)
                    if self.__period:
                        self.autocommit_timer = threading.Timer(self.__period, self.commit)
                        self.autocommit_timer.start()

    def update_documents(self, docs):
        count = 0

        with self.lock():
            for fields in docs:
                self.__writer.update_document(**fields)
                count += + 1
                self.__logger.debug(fields)

        self.__limit_counter += count

        return count

    def delete_documents(self, doc_ids, doc_id_field='id'):
        count = 0

        with self.lock():
            for doc_id in doc_ids:
                count += self.__writer.delete_by_term(doc_id_field, doc_id)
                self.__logger.debug(doc_id)

        self.__limit_counter += count

        return count

    def rollback(self):
        with self.lock():
            self.__writer.cancel()

    def optimize(self):
        self.commit(optimize=True, restart=True)

    def close(self):
        self.commit(restart=False)

    def is_closed(self):
        return self.__writer.is_closed

    def lock(self):
        return self.__lock
