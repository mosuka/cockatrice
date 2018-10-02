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

from pysyncobj import SyncObj, SyncObjConf
from pysyncobj.batteries import ReplDict


class KeyValueStore(SyncObj):
    def __init__(self, addr, peer_addrs, dump_file=None, logger=getLogger(__name__)):
        self.logger = logger

        self.conf = SyncObjConf(
            fullDumpFile=dump_file,
            logCompactionMinTime=30,
            dynamicMembershipChange=True
        )

        os.makedirs(os.path.dirname(dump_file), exist_ok=True)

        self.data = ReplDict()

        self.syncObj = SyncObj(addr, peer_addrs, conf=self.conf, consumers=[self.data])

    def set(self, key, value, sync=False):
        self.data.set(key, value, sync=sync)
        self.logger.debug("put: key={0} value={1}".format(key, value))

    def delete(self, key, sync=False):
        self.data.pop(key, sync=sync)
        self.logger.debug("delete: key={0}".format(key))

    def get(self, key):
        self.logger.debug("get: key={0}".format(key))
        return self.data.get(key)

    def len(self):
        ll = len(self.data.keys())
        self.logger.debug("len: {0}".format(ll))
        return ll
