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

from logging import getLogger

from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf

from cockatrice import NAME

LOGGER = getLogger(NAME)
HTTP_LOGGER = getLogger(NAME + '_http')
METRICS_REGISTRY = CollectorRegistry()
SYNC_CONFIG = SyncObjConf()
HOST = '127.0.0.1'
PORT = 7070
BIND_ADDR = '{0}:{1}'.format(HOST, PORT)
PEER_ADDRS = []
LOG_COMPACTION_MIN_ENTRIES = 5000
LOG_COMPACTION_MIN_TIME = 300
INDEX_DIR = '/tmp/cockatrice/index'
SNAPSHOT_FILE = '/tmp/cockatrice/snapshot.zip'
HTTP_PORT = 8080
LOG_LEVEL = 'DEBUG'
