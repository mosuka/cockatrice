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

import unittest

import os

from basilisk.schema import Schema


class TestSchema(unittest.TestCase):
    def setUp(self):
        self.conf_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), "../conf"))

        schema_file = self.conf_dir + "/schema.yaml"
        self.schema = Schema(schema_file)

    def test_get_unique_field(self):
        expected = "id"
        actual = self.schema.get_unique_field()

        self.assertEqual(expected, actual)
