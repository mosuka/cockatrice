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

from whoosh.fields import Schema as WhooshSchema

from cockatrice.util.loader import get_instance


class Schema(WhooshSchema):
    def __init__(self, scheme_dict):
        super().__init__()

        self.__schema_dict = scheme_dict

        try:
            for field_name in self.__schema_dict['schema'].keys():
                field_type = self.__get_field_type(self.__schema_dict['schema'][field_name]['field_type'])
                for arg in self.__schema_dict['schema'][field_name]['args'].keys():
                    setattr(field_type, arg, self.__schema_dict['schema'][field_name]['args'][arg])
                self.add(field_name, field_type, glob=False)

            if not self.__validate():
                raise ValueError('invalid schema')
        except Exception as ex:
            raise ex

    def __get_filter(self, name):
        class_name = self.__schema_dict['filters'][name]['class']
        class_args = {}
        if 'args' in self.__schema_dict['filters'][name]:
            class_args = deepcopy(self.__schema_dict['filters'][name]['args'])

        instance = get_instance(class_name, **class_args)

        return instance

    def __get_tokenizer(self, name):
        class_name = self.__schema_dict['tokenizers'][name]['class']
        class_args = {}
        if 'args' in self.__schema_dict['tokenizers'][name]:
            class_args = deepcopy(self.__schema_dict['tokenizers'][name]['args'])

        instance = get_instance(class_name, **class_args)

        return instance

    def __get_analyzer(self, name):
        instance = None

        if 'class' in self.__schema_dict['analyzers'][name]:
            class_name = self.__schema_dict['analyzers'][name]['class']
            class_args = {}
            if 'args' in self.__schema_dict['analyzers'][name]:
                class_args = deepcopy(self.__schema_dict['analyzers'][name]['args'])

            instance = get_instance(class_name, **class_args)
        elif 'tokenizer' in self.__schema_dict['analyzers'][name]:
            instance = self.__get_tokenizer(self.__schema_dict['analyzers'][name]['tokenizer'])
            if 'filters' in self.__schema_dict['analyzers'][name]:
                for filter_name in self.__schema_dict['analyzers'][name]['filters']:
                    instance = instance | self.__get_filter(filter_name)

        return instance

    def __get_field_type(self, name):
        class_name = self.__schema_dict['field_types'][name]['class']
        class_args = {}
        if 'args' in self.__schema_dict['field_types'][name]:
            class_args = deepcopy(self.__schema_dict['field_types'][name]['args'])
            if 'analyzer' in class_args:
                class_args['analyzer'] = self.__get_analyzer(class_args['analyzer']) if class_args['analyzer'] else None
            if 'tokenizer' in class_args:
                class_args['tokenizer'] = self.__get_tokenizer(class_args['tokenizer']) if class_args[
                    'tokenizer'] else None

        instance = get_instance(class_name, **class_args)

        return instance

    def __get_unique_fields(self):
        return [name for name, field in self.items() if field.unique]

    def __validate(self):
        valid = False

        if len(self.__get_unique_fields()) == 1:
            valid = True

        return valid

    def get_doc_id_field(self):
        return self.__get_unique_fields()[0]

    def get_default_search_field(self):
        return self.__schema_dict['default_search_field']
