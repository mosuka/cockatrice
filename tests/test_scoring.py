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
import unittest

from cockatrice.scoring import MultiWeighting


class TestMultiWeighting(unittest.TestCase):
    def setUp(self):
        self.example_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '../example'))

    def tearDown(self):
        pass

    def test_create_weighting(self):
        weighting_file = self.example_dir + '/weighting.yaml'

        with open(weighting_file, 'r', encoding='utf-8') as file_obj:
            weighting_yaml = file_obj.read()

        weighting = MultiWeighting(weighting_yaml)

        self.assertIsNotNone(weighting)
