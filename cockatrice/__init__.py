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

NAME = 'cockatrice'
VERSION = '0.6.0'

DEFAULT_LOGGER = getLogger(NAME)
DEFAULT_HTTP_LOGGER = getLogger(NAME + '_http')
DEFAULT_METRICS_REGISTRY = CollectorRegistry()
DEFAULT_SYNC_CONFIG = SyncObjConf()
DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 7070
DEFAULT_BIND_ADDR = '{0}:{1}'.format(DEFAULT_HOST, DEFAULT_PORT)
DEFAULT_PEER_ADDRS = []
DEFAULT_LOG_COMPACTION_MIN_ENTRIES = 5000
DEFAULT_LOG_COMPACTION_MIN_TIME = 300
DEFAULT_INDEX_DIR = '/tmp/cockatrice/index'
DEFAULT_SNAPSHOT_FILE = '/tmp/cockatrice/snapshot.zip'
DEFAULT_HTTP_PORT = 8080
DEFAULT_LOG_LEVEL = 'DEBUG'
