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

import json
import os
import signal
import sys
from http import HTTPStatus
from logging import CRITICAL, DEBUG, ERROR, Formatter, getLogger, INFO, NOTSET, StreamHandler, WARNING
from logging.handlers import RotatingFileHandler

import requests
import yaml
from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf
from yaml.constructor import ConstructorError

from cockatrice import NAME
from cockatrice.indexer import Indexer


def start_indexer(host='localhost', port=7070, seed_addr=None, snapshot_file='/tmp/cockatrice/snapshot.zip',
                  log_compaction_min_entries=5000, log_compaction_min_time=300, index_dir='/tmp/cockatrice/index',
                  grpc_port=5050, grpc_max_workers=10, http_port=8080, log_level='DEBUG', log_file=None,
                  log_file_max_bytes=512000000, log_file_backup_count=5, http_log_file=None,
                  http_log_file_max_bytes=512000000, http_log_file_backup_count=5):
    # create logger and handler
    logger = getLogger(NAME)
    log_handler = StreamHandler()

    # determine log destination
    if log_file is not None:
        log_handler = RotatingFileHandler(log_file, 'a+', maxBytes=log_file_max_bytes,
                                          backupCount=log_file_backup_count)

    # determine log level
    if log_level in ['CRITICAL', 'FATAL']:
        logger.setLevel(CRITICAL)
        log_handler.setLevel(CRITICAL)
    elif log_level == 'ERROR':
        logger.setLevel(ERROR)
        log_handler.setLevel(ERROR)
    elif log_level in ['WARNING', 'WARN']:
        logger.setLevel(WARNING)
        log_handler.setLevel(WARNING)
    elif log_level == 'INFO':
        logger.setLevel(INFO)
        log_handler.setLevel(INFO)
    elif log_level == 'DEBUG':
        logger.setLevel(DEBUG)
        log_handler.setLevel(DEBUG)
    elif log_level == 'NOTSET':
        logger.setLevel(NOTSET)
        log_handler.setLevel(NOTSET)
    else:
        logger.setLevel(INFO)
        log_handler.setLevel(INFO)

    # set log format
    handler_format = Formatter('%(asctime)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s')
    log_handler.setFormatter(handler_format)

    # add log handler
    logger.addHandler(log_handler)

    # create http logger and handler
    http_logger = getLogger(NAME + '_http')
    http_log_handler = StreamHandler()

    # determine http log destination
    if http_log_file is not None:
        http_log_handler = RotatingFileHandler(http_log_file, 'a+', maxBytes=http_log_file_max_bytes,
                                               backupCount=http_log_file_backup_count)

    # determine http log level
    http_logger.setLevel(INFO)
    http_log_handler.setLevel(INFO)

    # set http log format
    http_handler_format = Formatter('%(message)s')
    http_log_handler.setFormatter(http_handler_format)

    # add http log handler
    http_logger.addHandler(http_log_handler)

    # metrics registry
    metrics_registry = CollectorRegistry()

    # sync config
    os.makedirs(os.path.dirname(snapshot_file), exist_ok=True)
    conf = SyncObjConf()
    conf.fullDumpFile = snapshot_file
    conf.logCompactionMinEntries = log_compaction_min_entries
    conf.logCompactionMinTime = log_compaction_min_time
    conf.dynamicMembershipChange = True
    conf.validate()

    indexer = None
    try:
        indexer = Indexer(host=host, port=port, seed_addr=seed_addr, conf=conf, index_dir=index_dir,
                          grpc_port=grpc_port, grpc_max_workers=grpc_max_workers, http_port=http_port, logger=logger,
                          http_logger=http_logger, metrics_registry=metrics_registry)
        while True:
            signal.pause()
    except Exception as ex:
        print(ex)
    finally:
        if indexer is not None:
            indexer.stop()


def create_index(index_name, schema_file, host='localhost', port=8080, output='yaml', sync=False):
    try:
        with open(schema_file) as f:
            schema_data = f.read()

        content_type = ''
        try:
            json.loads(schema_data)
            content_type = 'application/json'
        except json.decoder.JSONDecodeError:
            pass
        try:
            yaml.safe_load(schema_data)
            content_type = 'application/yaml'
        except yaml.constructor.ConstructorError:
            pass

        response = requests.put(
            'http://{0}:{1}/indices/{2}?output={3}&sync={4}'.format(host, port, index_name, output, sync),
            headers={'Content-Type': content_type},
            data=schema_data)
        print(response.text)
    except Exception as ex:
        print(ex)


def get_index(index_name, host='localhost', port=8080, output='yaml'):
    try:
        response = requests.get('http://{0}:{1}/indices/{2}?output={3}'.format(host, port, index_name, output))
        print(response.text)
    except Exception as ex:
        print(ex)


def delete_index(index_name, host='localhost', port=8080, output='yaml', sync=False):
    try:
        response = requests.delete(
            'http://{0}:{1}/indices/{2}?output={3}&sync={4}'.format(host, port, index_name, output, sync))
        print(response.text)
    except Exception as ex:
        print(ex)


def put_document(index_name, document_id, fields_file, host='localhost', port=8080, output='yaml', sync=False):
    try:
        with open(fields_file) as f:
            fields_data = f.read()

        content_type = ''
        try:
            json.loads(fields_data)
            content_type = 'application/json'
        except json.decoder.JSONDecodeError:
            pass
        try:
            yaml.safe_load(fields_data)
            content_type = 'application/yaml'
        except yaml.constructor.ConstructorError:
            pass

        response = requests.put(
            'http://{0}:{1}/indices/{2}/documents/{3}?output={4}&sync={5}'.format(host, port, index_name, document_id,
                                                                                  output, sync),
            headers={'Content-Type': content_type},
            data=fields_data)
        print(response.text)
    except Exception as ex:
        print(ex)


def get_document(index_name, document_id, host='localhost', port=8080, output='yaml'):
    try:
        response = requests.get(
            'http://{0}:{1}/indices/{2}/documents/{3}?output={4}'.format(host, port, index_name, document_id, output))
        print(response.text)
    except Exception as ex:
        print(ex)


def delete_document(index_name, document_id, host='localhost', port=8080, output='yaml', sync=False):
    try:
        response = requests.delete(
            'http://{0}:{1}/indices/{2}/documents/{3}?output={4}&sync={5}'.format(host, port, index_name, document_id,
                                                                                  output, sync))
        print(response.text)
    except Exception as ex:
        print(ex)


def put_documents(index_name, documents_file, host='localhost', port=8080, output='yaml', sync=False):
    try:
        with open(documents_file) as f:
            documents_data = f.read()

        content_type = ''
        try:
            json.loads(documents_data)
            content_type = 'application/json'
        except json.decoder.JSONDecodeError:
            pass
        try:
            yaml.safe_load(documents_data)
            content_type = 'application/yaml'
        except yaml.constructor.ConstructorError:
            pass

        response = requests.put(
            'http://{0}:{1}/indices/{2}/documents?output={3}&sync={4}'.format(host, port, index_name, output, sync),
            headers={'Content-Type': content_type},
            data=documents_data)
        print(response.text)
    except Exception as ex:
        print(ex)


def delete_documents(index_name, document_ids_file, host='localhost', port=8080, output='yaml', sync=False):
    try:
        with open(document_ids_file) as f:
            document_ids_data = f.read()

        content_type = ''
        try:
            json.loads(document_ids_data)
            content_type = 'application/json'
        except json.decoder.JSONDecodeError:
            pass
        try:
            yaml.safe_load(document_ids_data)
            content_type = 'application/yaml'
        except yaml.constructor.ConstructorError:
            pass

        response = requests.delete(
            'http://{0}:{1}/indices/{2}/documents?output={3}&sync={4}'.format(host, port, index_name, output, sync),
            headers={'Content-Type': content_type},
            data=document_ids_data)
        print(response.text)
    except Exception as ex:
        print(ex)


def search(index_name, query, page_num=1, page_len=10, weighting_file=None, host='localhost', port=8080, output='yaml'):
    try:
        if weighting_file is None:
            response = requests.get(
                'http://{0}:{1}/indices/{2}/search?query={3}&page_num={4}&page_len={5}&output={6}'.format(host, port,
                                                                                                          index_name,
                                                                                                          query,
                                                                                                          page_num,
                                                                                                          page_len,
                                                                                                          output))
        else:
            with open(weighting_file) as f:
                weighting_data = f.read()

            content_type = ''
            try:
                json.loads(weighting_data)
                content_type = 'application/json'
            except json.decoder.JSONDecodeError:
                pass
            try:
                yaml.safe_load(weighting_data)
                content_type = 'application/yaml'
            except yaml.constructor.ConstructorError:
                pass

            response = requests.post(
                'http://{0}:{1}/indices/{2}/search?query={3}&page_num={4}&page_len={5}&output={6}'.format(host, port,
                                                                                                          index_name,
                                                                                                          query,
                                                                                                          page_num,
                                                                                                          page_len,
                                                                                                          output),
                headers={'Content-Type': content_type},
                data=weighting_data)
        print(response.text)
    except Exception as ex:
        print(ex)


def add_node(node_addr, host='localhost', port=8080, output='yaml'):
    try:
        response = requests.put(
            'http://{0}:{1}/nodes/{2}?output={3}'.format(host, port, node_addr, output))
        print(response.text)
    except Exception as ex:
        print(ex)


def delete_node(node_addr, host='localhost', port=8080, output='yaml'):
    try:
        response = requests.delete(
            'http://{0}:{1}/nodes/{2}?output={3}'.format(host, port, node_addr, output))
        print(response.text)
    except Exception as ex:
        print(ex)


def create_snapshot(host='localhost', port=8080, output='yaml'):
    try:
        response = requests.put(
            'http://{0}:{1}/snapshot?output={2}'.format(host, port, output))
        print(response.text)
    except Exception as ex:
        print(ex)


def get_snapshot(host='localhost', port=8080, output_file=None):
    try:
        response = requests.get('http://{0}:{1}/snapshot'.format(host, port))
        if response.status_code == HTTPStatus.OK:
            if output_file is None:
                sys.stdout.write(response.text)
            else:
                with open(output_file, 'wb') as f:
                    f.write(response.content)
    except Exception as ex:
        print(ex)


def healthiness(host='localhost', port=8080, output='yaml'):
    try:
        response = requests.get('http://{0}:{1}/healthiness?output={2}'.format(host, port, output))
        print(response.text)
    except Exception as ex:
        print(ex)


def liveness(host='localhost', port=8080, output='yaml'):
    try:
        response = requests.get('http://{0}:{1}/liveness?output={2}'.format(host, port, output))
        print(response.text)
    except Exception as ex:
        print(ex)


def readiness(host='localhost', port=8080, output='yaml'):
    try:
        response = requests.get('http://{0}:{1}/readiness?output={2}'.format(host, port, output))
        print(response.text)
    except Exception as ex:
        print(ex)


def status(host='localhost', port=8080, output='yaml'):
    try:
        response = requests.get('http://{0}:{1}/status?output={2}'.format(host, port, output))
        print(response.text)
    except Exception as ex:
        print(ex)
