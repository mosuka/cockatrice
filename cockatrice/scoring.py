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

from copy import deepcopy

import yaml
from whoosh.scoring import MultiWeighting as WhooshMultiWeighting, BM25F

from cockatrice.util.loader import get_instance


class MultiWeighting(WhooshMultiWeighting):
    def __init__(self, weighting_yaml):
        self.__dict = {}

        default = None
        weightings = {}

        try:
            self.__dict = yaml.safe_load(weighting_yaml)
            for field_name in self.__dict['weighting'].keys():
                if field_name == 'default':
                    default = self.__get_weighting(field_name)
                else:
                    weightings[field_name] = self.__get_weighting(field_name)
            if default is None:
                default = BM25F(B=0.75, K1=1.2)
            super().__init__(default, **weightings)
        except Exception as ex:
            raise ex

    def __get_weighting(self, name):
        class_name = self.__dict['weighting'][name]['class']
        class_args = {}
        if 'args' in self.__dict['weighting'][name]:
            class_args = deepcopy(self.__dict['weighting'][name]['args'])

        instance = get_instance(class_name, **class_args)

        return instance
