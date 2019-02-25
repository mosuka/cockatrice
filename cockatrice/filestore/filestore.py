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

import os
from tempfile import TemporaryDirectory

from whoosh.filedb.filestore import FileStorage, RamStorage as WhooshRamStorage
from whoosh.util import random_name


class RamStorage(WhooshRamStorage):
    """Storage object that keeps the index in memory.
    """

    def __init__(self):
        super().__init__()

        self.tmp_dir = TemporaryDirectory()

    def temp_storage(self, name=None):
        tdir = self.tmp_dir.name
        name = name or "%s.tmp" % random_name()
        path = os.path.join(tdir, name)
        tempstore = FileStorage(path)
        return tempstore.create()
