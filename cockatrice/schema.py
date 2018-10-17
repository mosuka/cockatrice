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

import importlib
from copy import deepcopy

import yaml
from whoosh.fields import Schema as WhooshSchema


def get_instance(class_name, **class_args):
    class_data = class_name.split('.')

    module_path = '.'.join(class_data[:-1])
    class_name = class_data[-1]

    module = importlib.import_module(module_path)
    class_obj = getattr(module, class_name)

    if class_args:
        return class_obj(**class_args)
    else:
        return class_obj()


class Schema(WhooshSchema):
    def __init__(self, scheme_yaml):
        super().__init__()

        self.__dict = {}

        try:
            self.__dict = yaml.safe_load(scheme_yaml)

            for field_name in self.__dict['schema'].keys():
                field_type = self.__get_field_type(self.__dict['schema'][field_name]['field_type'])
                for arg in self.__dict['schema'][field_name]['args'].keys():
                    setattr(field_type, arg, self.__dict['schema'][field_name]['args'][arg])
                self.add(field_name, field_type, glob=False)
        except Exception as ex:
            raise ex

    def __get_filter(self, name):
        class_name = self.__dict['filters'][name]['class']
        class_args = {}
        if 'args' in self.__dict['filters'][name]:
            class_args = deepcopy(self.__dict['filters'][name]['args'])

        instance = get_instance(class_name, **class_args)

        return instance

    def __get_tokenizer(self, name):
        class_name = self.__dict['tokenizers'][name]['class']
        class_args = {}
        if 'args' in self.__dict['tokenizers'][name]:
            class_args = deepcopy(self.__dict['tokenizers'][name]['args'])

        instance = get_instance(class_name, **class_args)

        return instance

    def __get_analyzer(self, name):
        instance = None

        if 'class' in self.__dict['analyzers'][name]:
            class_name = self.__dict['analyzers'][name]['class']
            class_args = {}
            if 'args' in self.__dict['analyzers'][name]:
                class_args = deepcopy(self.__dict['analyzers'][name]['args'])

            instance = get_instance(class_name, **class_args)
        elif 'tokenizer' in self.__dict['analyzers'][name]:
            instance = self.__get_tokenizer(self.__dict['analyzers'][name]['tokenizer'])
            if 'filters' in self.__dict['analyzers'][name]:
                for filter_name in self.__dict['analyzers'][name]['filters']:
                    instance = instance | self.__get_filter(filter_name)

        return instance

    def __get_field_type(self, name):
        class_name = self.__dict['field_types'][name]['class']
        class_args = {}
        if 'args' in self.__dict['field_types'][name]:
            class_args = deepcopy(self.__dict['field_types'][name]['args'])
            if 'analyzer' in class_args:
                class_args['analyzer'] = self.__get_analyzer(class_args['analyzer']) if class_args['analyzer'] else None
            if 'tokenizer' in class_args:
                class_args['tokenizer'] = self.__get_tokenizer(class_args['tokenizer']) if class_args[
                    'tokenizer'] else None

        instance = get_instance(class_name, **class_args)

        return instance

    def get_unique_field(self):
        for name, obj in self.items():
            if obj.unique:
                return name

        return None

    def get_default_search_field(self):
        return self.__dict['default_search_field']
