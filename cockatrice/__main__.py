#!/usr/bin/env python

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

import signal
import sys
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser

from cockatrice import VERSION
from cockatrice.cli import add_node, commit, create_index, create_snapshot, delete_document, delete_documents, \
    delete_index, delete_node, get_document, get_index, get_snapshot, healthiness, liveness, optimize, put_document, \
    put_documents, readiness, rollback, search, start_indexer, start_manager, status


def signal_handler(signal, frame):
    sys.exit(0)


def start_manager_handler(args):
    start_manager(host=args.host, port=args.port, peer_addr=args.peer_addr, snapshot_file=args.snapshot_file,
                  log_compaction_min_entries=args.log_compaction_min_entries,
                  log_compaction_min_time=args.log_compaction_min_time, data_dir=args.data_dir,
                  grpc_port=args.grpc_port, grpc_max_workers=args.grpc_max_workers, http_port=args.http_port,
                  log_level=args.log_level, log_file=args.log_file, log_file_max_bytes=args.log_file_max_bytes,
                  log_file_backup_count=args.log_file_backup_count, http_log_file=args.http_log_file,
                  http_log_file_max_bytes=args.http_log_file_max_bytes,
                  http_log_file_backup_count=args.http_log_file_backup_count)


def start_indexer_handler(args):
    start_indexer(host=args.host, port=args.port, peer_addr=args.peer_addr, snapshot_file=args.snapshot_file,
                  log_compaction_min_entries=args.log_compaction_min_entries,
                  log_compaction_min_time=args.log_compaction_min_time, data_dir=args.data_dir,
                  grpc_port=args.grpc_port, grpc_max_workers=args.grpc_max_workers, http_port=args.http_port,
                  log_level=args.log_level, log_file=args.log_file, log_file_max_bytes=args.log_file_max_bytes,
                  log_file_backup_count=args.log_file_backup_count, http_log_file=args.http_log_file,
                  http_log_file_max_bytes=args.http_log_file_max_bytes,
                  http_log_file_backup_count=args.http_log_file_backup_count)


def create_index_handler(args):
    create_index(args.index_name, args.schema, host=args.host, port=args.port, output=args.output, sync=args.sync)


def get_index_handler(args):
    get_index(args.index_name, host=args.host, port=args.port, output=args.output)


def delete_index_handler(args):
    delete_index(args.index_name, host=args.host, port=args.port, output=args.output, sync=args.sync)


def put_document_handler(args):
    put_document(args.index_name, args.document_id, args.document_fields, host=args.host, port=args.port,
                 output=args.output, sync=args.sync)


def get_document_handler(args):
    get_document(args.index_name, args.document_id, host=args.host, port=args.port, output=args.output)


def delete_document_handler(args):
    delete_document(args.index_name, args.document_id, host=args.host, port=args.port, output=args.output,
                    sync=args.sync)


def put_documents_handler(args):
    put_documents(args.index_name, args.documents, host=args.host, port=args.port, output=args.output, sync=args.sync)


def delete_documents_handler(args):
    delete_documents(args.index_name, args.document_ids, host=args.host, port=args.port, output=args.output,
                     sync=args.sync)


def commit_handler(args):
    commit(args.index_name, host=args.host, port=args.port, output=args.output)


def rollback_handler(args):
    rollback(args.index_name, host=args.host, port=args.port, output=args.output)


def optimize_handler(args):
    optimize(args.index_name, host=args.host, port=args.port, output=args.output)


def search_handler(args):
    search(args.index_name, args.query, page_num=args.page_num, page_len=args.page_len,
           weighting_file=args.weighting_file, host=args.host, port=args.port, output=args.output)


def add_node_handler(args):
    add_node(args.node_addr, host=args.host, port=args.port, output=args.output)


def delete_node_handler(args):
    delete_node(args.node_addr, host=args.host, port=args.port, output=args.output)


def create_snapshot_handler(args):
    create_snapshot(host=args.host, port=args.port)


def get_snapshot_handler(args):
    get_snapshot(host=args.host, port=args.port, output_file=args.output_file)


def healthiness_handler(args):
    healthiness(host=args.host, port=args.port, output=args.output)


def liveness_handler(args):
    liveness(host=args.host, port=args.port, output=args.output)


def readiness_handler(args):
    readiness(host=args.host, port=args.port, output=args.output)


def status_handler(args):
    status(host=args.host, port=args.port, output=args.output)


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGQUIT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = ArgumentParser(description='cockatrice command', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--version', action='version', version='cockatrice {0}'.format(VERSION))

    subparsers = parser.add_subparsers()

    # start
    parser_start = subparsers.add_parser('start', help='see `start --help`',
                                         formatter_class=ArgumentDefaultsHelpFormatter)
    start_subparser = parser_start.add_subparsers()

    # start supervisor
    parser_start_manager = start_subparser.add_parser('manager', help='see `manager --help`',
                                                      formatter_class=ArgumentDefaultsHelpFormatter)
    parser_start_manager.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                      help='the host address to listen on for peer traffic')
    parser_start_manager.add_argument('--port', dest='port', default=7070, metavar='PORT', type=int,
                                      help='the port to listen on for peer traffic')
    parser_start_manager.add_argument('--peer-addr', dest='peer_addr', default=None, metavar='PEER_ADDR', type=str,
                                      help='the address of the peer node in the existing cluster')
    parser_start_manager.add_argument('--snapshot-file', dest='snapshot_file',
                                      default='/tmp/cockatrice/management.zip',
                                      metavar='SNAPSHOT_FILE', type=str,
                                      help='file to store snapshot of all indices')
    parser_start_manager.add_argument('--log-compaction-min-entries', dest='log_compaction_min_entries',
                                      default=5000,
                                      metavar='LOG_COMPACTION_MIN_ENTRIES', type=int,
                                      help='log-compaction interval min entries')
    parser_start_manager.add_argument('--log-compaction-min-time', dest='log_compaction_min_time', default=300,
                                      metavar='LOG_COMPACTION_MIN_TIME', type=int,
                                      help='log-compaction interval min time in seconds')
    parser_start_manager.add_argument('--data-dir', dest='data_dir', default='/tmp/cockatrice/management',
                                      metavar='DATA_DIR', type=str, help='data dir')
    parser_start_manager.add_argument('--grpc-port', dest='grpc_port', default=5050, metavar='GRPC_PORT', type=int,
                                      help='the port to listen on for gRPC traffic')
    parser_start_manager.add_argument('--grpc-max-workers', dest='grpc_max_workers', default=10,
                                      metavar='GRPC_MAX_WORKERS', type=int,
                                      help='the number of workers for gRPC server')
    parser_start_manager.add_argument('--http-port', dest='http_port', default=8080,
                                      metavar='HTTP_PORT', type=int, help='the port to listen on for HTTP traffic')
    parser_start_manager.add_argument('--log-level', dest='log_level', default='DEBUG', metavar='LOG_LEVEL',
                                      type=str,
                                      help='log level')
    parser_start_manager.add_argument('--log-file', dest='log_file', default=None, metavar='LOG_FILE', type=str,
                                      help='log file')
    parser_start_manager.add_argument('--log-file-max-bytes', dest='log_file_max_bytes', default=512000000,
                                      metavar='LOG_FILE_MAX_BYTES', type=int, help='log file max bytes')
    parser_start_manager.add_argument('--log-file-backup-count', dest='log_file_backup_count', default=5,
                                      metavar='LOG_FILE_BACKUP_COUNT', type=int, help='log file backup count')
    parser_start_manager.add_argument('--http-log-file', dest='http_log_file', default=None, metavar='HTTP_LOG_FILE',
                                      type=str, help='http log file')
    parser_start_manager.add_argument('--http-log-file-max-bytes', dest='http_log_file_max_bytes', default=512000000,
                                      metavar='HTTP_LOG_FILE_MAX_BYTES', type=int, help='http log file max bytes')
    parser_start_manager.add_argument('--http-log-file-backup-count', dest='http_log_file_backup_count', default=5,
                                      metavar='HTTP_LOG_FILE_BACKUP_COUNT', type=int,
                                      help='http log file backup count')
    parser_start_manager.set_defaults(handler=start_manager_handler)

    # start indexer
    parser_start_indexer = start_subparser.add_parser('indexer', help='see `indexer --help`',
                                                      formatter_class=ArgumentDefaultsHelpFormatter)
    parser_start_indexer.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                      help='the host address to listen on for peer traffic')
    parser_start_indexer.add_argument('--port', dest='port', default=7070, metavar='PORT', type=int,
                                      help='the port to listen on for peer traffic')
    parser_start_indexer.add_argument('--peer-addr', dest='peer_addr', default=None, metavar='PEER_ADDR', type=str,
                                      help='the address of the peer node in the existing cluster')
    parser_start_indexer.add_argument('--snapshot-file', dest='snapshot_file', default='/tmp/cockatrice/index.zip',
                                      metavar='SNAPSHOT_FILE', type=str, help='file to store snapshot of all indices')
    parser_start_indexer.add_argument('--log-compaction-min-entries', dest='log_compaction_min_entries', default=5000,
                                      metavar='LOG_COMPACTION_MIN_ENTRIES', type=int,
                                      help='log-compaction interval min entries')
    parser_start_indexer.add_argument('--log-compaction-min-time', dest='log_compaction_min_time', default=300,
                                      metavar='LOG_COMPACTION_MIN_TIME', type=int,
                                      help='log-compaction interval min time in seconds')
    parser_start_indexer.add_argument('--data-dir', dest='data_dir', default='/tmp/cockatrice/index',
                                      metavar='DATA_DIR', type=str, help='data dir')
    parser_start_indexer.add_argument('--grpc-port', dest='grpc_port', default=5050, metavar='GRPC_PORT', type=int,
                                      help='the port to listen on for gRPC traffic')
    parser_start_indexer.add_argument('--grpc-max-workers', dest='grpc_max_workers', default=10,
                                      metavar='GRPC_MAX_WORKERS', type=int,
                                      help='the number of workers for gRPC server')
    parser_start_indexer.add_argument('--http-port', dest='http_port', default=8080,
                                      metavar='HTTP_PORT', type=int, help='the port to listen on for HTTP traffic')
    parser_start_indexer.add_argument('--log-level', dest='log_level', default='DEBUG', metavar='LOG_LEVEL', type=str,
                                      help='log level')
    parser_start_indexer.add_argument('--log-file', dest='log_file', default=None, metavar='LOG_FILE', type=str,
                                      help='log file')
    parser_start_indexer.add_argument('--log-file-max-bytes', dest='log_file_max_bytes', default=512000000,
                                      metavar='LOG_FILE_MAX_BYTES', type=int, help='log file max bytes')
    parser_start_indexer.add_argument('--log-file-backup-count', dest='log_file_backup_count', default=5,
                                      metavar='LOG_FILE_BACKUP_COUNT', type=int, help='log file backup count')
    parser_start_indexer.add_argument('--http-log-file', dest='http_log_file', default=None, metavar='HTTP_LOG_FILE',
                                      type=str, help='http log file')
    parser_start_indexer.add_argument('--http-log-file-max-bytes', dest='http_log_file_max_bytes', default=512000000,
                                      metavar='HTTP_LOG_FILE_MAX_BYTES', type=int, help='http log file max bytes')
    parser_start_indexer.add_argument('--http-log-file-backup-count', dest='http_log_file_backup_count', default=5,
                                      metavar='HTTP_LOG_FILE_BACKUP_COUNT', type=int, help='http log file backup count')
    parser_start_indexer.set_defaults(handler=start_indexer_handler)

    # create
    parser_create = subparsers.add_parser('create', help='see `create --help`',
                                          formatter_class=ArgumentDefaultsHelpFormatter)
    create_subparser = parser_create.add_subparsers()

    # create index
    parser_create_index = create_subparser.add_parser('index', help='see `index --help`',
                                                      formatter_class=ArgumentDefaultsHelpFormatter)
    parser_create_index.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                     help='the host address to listen on for http traffic')
    parser_create_index.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                                     help='the port to listen on for HTTP traffic')
    parser_create_index.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                                     help='the output format')
    parser_create_index.add_argument('--sync', dest='sync', default=False, metavar='SYNC', type=bool,
                                     help='wait for synchronize data')
    parser_create_index.add_argument('index_name', metavar='INDEX_NAME', type=str, help='the index name')
    parser_create_index.add_argument('schema', metavar='SCHEMA', type=str, help='the schema for the index')
    parser_create_index.set_defaults(handler=create_index_handler)

    # create snapshot
    parser_create_snapshot = create_subparser.add_parser('snapshot', help='see `snapshot --help`',
                                                         formatter_class=ArgumentDefaultsHelpFormatter)
    parser_create_snapshot.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                        help='the host address to listen on for http traffic')
    parser_create_snapshot.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                                        help='the port to listen on for HTTP traffic')
    parser_create_snapshot.set_defaults(handler=create_snapshot_handler)

    # add
    parser_add = subparsers.add_parser('add', help='see `add --help`', formatter_class=ArgumentDefaultsHelpFormatter)
    add_subparser = parser_add.add_subparsers()

    # add node
    parser_add_node = add_subparser.add_parser('node', help='see `node --help`',
                                               formatter_class=ArgumentDefaultsHelpFormatter)
    parser_add_node.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                 help='the host address to listen on for http traffic')
    parser_add_node.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                                 help='the port to listen on for HTTP traffic')
    parser_add_node.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                                 help='the output format')
    parser_add_node.add_argument('node_addr', metavar='NODE_ADDR', type=str,
                                 help='the address of node to add to the cluster')
    parser_add_node.set_defaults(handler=add_node_handler)

    # put
    parser_put = subparsers.add_parser('put', help='see `put --help`', formatter_class=ArgumentDefaultsHelpFormatter)
    put_subparser = parser_put.add_subparsers()

    # put document
    parser_put_document = put_subparser.add_parser('document', help='see `document --help`',
                                                   formatter_class=ArgumentDefaultsHelpFormatter)
    parser_put_document.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                     help='the host address to listen on for http traffic')
    parser_put_document.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                                     help='the port to listen on for HTTP traffic')
    parser_put_document.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                                     help='the output format')
    parser_put_document.add_argument('--sync', dest='sync', default=False, metavar='SYNC', type=bool,
                                     help='wait for synchronize data')
    parser_put_document.add_argument('index_name', metavar='INDEX_NAME', type=str, help='the index name')
    parser_put_document.add_argument('document_id', metavar='DOCUMENT_ID', type=str, help='the document id')
    parser_put_document.add_argument('document_fields', metavar='DOCUMENT_FIELDS', type=str, help='the document fields')
    parser_put_document.set_defaults(handler=put_document_handler)

    # put documents
    parser_put_documents = put_subparser.add_parser('documents', help='see `documents --help`',
                                                    formatter_class=ArgumentDefaultsHelpFormatter)
    parser_put_documents.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                      help='the host address to listen on for http traffic')
    parser_put_documents.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                                      help='the port to listen on for HTTP traffic')
    parser_put_documents.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                                      help='the output format')
    parser_put_documents.add_argument('--sync', dest='sync', default=False, metavar='SYNC', type=bool,
                                      help='wait for synchronize data')
    parser_put_documents.add_argument('index_name', metavar='INDEX_NAME', type=str, help='the index name')
    parser_put_documents.add_argument('documents', metavar='DOCUMENTS', type=str, help='the documents list')
    parser_put_documents.set_defaults(handler=put_documents_handler)

    # get
    parser_get = subparsers.add_parser('get', help='see `get --help`', formatter_class=ArgumentDefaultsHelpFormatter)
    get_subparser = parser_get.add_subparsers()

    # get index
    parser_get_index = get_subparser.add_parser('index', help='see `index --help`',
                                                formatter_class=ArgumentDefaultsHelpFormatter)
    parser_get_index.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                  help='the host address to listen on for http traffic')
    parser_get_index.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                                  help='the port to listen on for HTTP traffic')
    parser_get_index.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                                  help='the output format')
    parser_get_index.add_argument('index_name', metavar='INDEX_NAME', type=str, help='the index name')
    parser_get_index.set_defaults(handler=get_index_handler)

    # get document
    parser_get_document = get_subparser.add_parser('document', help='see `document --help`',
                                                   formatter_class=ArgumentDefaultsHelpFormatter)
    parser_get_document.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                     help='the host address to listen on for http traffic')
    parser_get_document.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                                     help='the port to listen on for HTTP traffic')
    parser_get_document.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                                     help='the output format')
    parser_get_document.add_argument('index_name', metavar='INDEX_NAME', type=str, help='the index name')
    parser_get_document.add_argument('document_id', metavar='DOCUMENT_ID', type=str, help='the document id')
    parser_get_document.set_defaults(handler=get_document_handler)

    # get snapshot
    parser_get_snapshot = get_subparser.add_parser('snapshot', help='see `snapshot --help`',
                                                   formatter_class=ArgumentDefaultsHelpFormatter)
    parser_get_snapshot.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                     help='the host address to listen on for http traffic')
    parser_get_snapshot.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                                     help='the port to listen on for HTTP traffic')
    parser_get_snapshot.add_argument('--output-file', dest='output_file', default=None, metavar='OUTPUT_FILE', type=str,
                                     help='the snapshot file destination')
    parser_get_snapshot.set_defaults(handler=get_snapshot_handler)

    # delete
    parser_delete = subparsers.add_parser('delete', help='see `delete --help`',
                                          formatter_class=ArgumentDefaultsHelpFormatter)
    delete_subparser = parser_delete.add_subparsers()

    # delete index
    parser_delete_index = delete_subparser.add_parser('index', help='see `index --help`',
                                                      formatter_class=ArgumentDefaultsHelpFormatter)
    parser_delete_index.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                     help='the host address to listen on for http traffic')
    parser_delete_index.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                                     help='the port to listen on for HTTP traffic')
    parser_delete_index.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                                     help='the output format')
    parser_delete_index.add_argument('--sync', dest='sync', default=False, metavar='SYNC', type=bool,
                                     help='wait for synchronize data')
    parser_delete_index.add_argument('index_name', metavar='INDEX_NAME', type=str, help='the index name')
    parser_delete_index.set_defaults(handler=delete_index_handler)

    # delete document
    parser_delete_document = delete_subparser.add_parser('document', help='see `document --help`',
                                                         formatter_class=ArgumentDefaultsHelpFormatter)
    parser_delete_document.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                        help='the host address to listen on for http traffic')
    parser_delete_document.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                                        help='the port to listen on for HTTP traffic')
    parser_delete_document.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                                        help='the output format')
    parser_delete_document.add_argument('--sync', dest='sync', default=False, metavar='SYNC', type=bool,
                                        help='wait for synchronize data')
    parser_delete_document.add_argument('index_name', metavar='INDEX_NAME', type=str, help='the index name')
    parser_delete_document.add_argument('document_id', metavar='DOCUMENT_ID', type=str, help='the document id')
    parser_delete_document.set_defaults(handler=delete_document_handler)

    # delete documents
    parser_delete_documents = delete_subparser.add_parser('documents', help='see `documents --help`',
                                                          formatter_class=ArgumentDefaultsHelpFormatter)
    parser_delete_documents.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                         help='the host address to listen on for http traffic')
    parser_delete_documents.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                                         help='the port to listen on for HTTP traffic')
    parser_delete_documents.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                                         help='the output format')
    parser_delete_documents.add_argument('--sync', dest='sync', default=False, metavar='SYNC', type=bool,
                                         help='wait for synchronize data')
    parser_delete_documents.add_argument('index_name', metavar='INDEX_NAME', type=str, help='the index name')
    parser_delete_documents.add_argument('document_ids', metavar='DOCUMENT_IDS', type=str, help='the document ID list')
    parser_delete_documents.set_defaults(handler=delete_documents_handler)

    # delete node
    parser_delete_node = delete_subparser.add_parser('node', help='see `node --help`',
                                                     formatter_class=ArgumentDefaultsHelpFormatter)
    parser_delete_node.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                    help='the host address to listen on for http traffic')
    parser_delete_node.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                                    help='the port to listen on for HTTP traffic')
    parser_delete_node.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                                    help='the output format')
    parser_delete_node.add_argument('node_addr', metavar='NODE_ADDR', type=str,
                                    help='the address of node to delete from the cluster')
    parser_delete_node.set_defaults(handler=delete_node_handler)

    # search
    parser_search = subparsers.add_parser('search', help='see `search --help`',
                                          formatter_class=ArgumentDefaultsHelpFormatter)
    parser_search.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                               help='the host address to listen on for http traffic')
    parser_search.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                               help='the port to listen on for HTTP traffic')
    parser_search.add_argument('--search-field', dest='search_field', default='', metavar='SEARCH_FIELD', type=str,
                               help='the default search field')
    parser_search.add_argument('--page-num', dest='page_num', default=1, metavar='PAGE_NUM', type=int,
                               help='the page number')
    parser_search.add_argument('--page-len', dest='page_len', default=10, metavar='PAGE_LEN', type=int,
                               help='the page length')
    parser_search.add_argument('--weighting-file', dest='weighting_file', default=None,
                               metavar='WEIGHTING_FILE', type=str, help='the weighting file')
    parser_search.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                               help='the output format')
    parser_search.add_argument('index_name', metavar='INDEX_NAME', type=str, help='the index name')
    parser_search.add_argument('query', metavar='QUERY', type=str, help='the query string')
    parser_search.set_defaults(handler=search_handler)

    # commit
    parser_commit = subparsers.add_parser('commit', help='see `commit --help`',
                                          formatter_class=ArgumentDefaultsHelpFormatter)
    parser_commit.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                               help='the host address to listen on for http traffic')
    parser_commit.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                               help='the port to listen on for HTTP traffic')
    parser_commit.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                               help='the output format')
    parser_commit.add_argument('index_name', metavar='INDEX_NAME', type=str, help='the index name')
    parser_commit.set_defaults(handler=commit_handler)

    # rollback
    parser_rollback = subparsers.add_parser('rollback', help='see `rollback --help`',
                                            formatter_class=ArgumentDefaultsHelpFormatter)
    parser_rollback.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                 help='the host address to listen on for http traffic')
    parser_rollback.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                                 help='the port to listen on for HTTP traffic')
    parser_rollback.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                                 help='the output format')
    parser_rollback.add_argument('index_name', metavar='INDEX_NAME', type=str, help='the index name')
    parser_rollback.set_defaults(handler=rollback_handler)

    # optimize
    parser_optimize = subparsers.add_parser('optimize', help='see `optimize --help`',
                                            formatter_class=ArgumentDefaultsHelpFormatter)
    parser_optimize.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                 help='the host address to listen on for http traffic')
    parser_optimize.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                                 help='the port to listen on for HTTP traffic')
    parser_optimize.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                                 help='the output format')
    parser_optimize.add_argument('index_name', metavar='INDEX_NAME', type=str, help='the index name')
    parser_optimize.set_defaults(handler=optimize_handler)

    # healthiness
    parser_healthiness = subparsers.add_parser('healthiness', help='see `healthiness --help`',
                                               formatter_class=ArgumentDefaultsHelpFormatter)
    parser_healthiness.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                    help='the host address to listen on for http traffic')
    parser_healthiness.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                                    help='the port to listen on for HTTP traffic')
    parser_healthiness.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                                    help='the output format')
    parser_healthiness.set_defaults(handler=healthiness_handler)

    # liveness
    parser_liveness = subparsers.add_parser('liveness', help='see `liveness --help`',
                                            formatter_class=ArgumentDefaultsHelpFormatter)
    parser_liveness.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                 help='the host address to listen on for http traffic')
    parser_liveness.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                                 help='the port to listen on for HTTP traffic')
    parser_liveness.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                                 help='the output format')
    parser_liveness.set_defaults(handler=liveness_handler)

    # readiness
    parser_readiness = subparsers.add_parser('readiness', help='see `readiness --help`',
                                             formatter_class=ArgumentDefaultsHelpFormatter)
    parser_readiness.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                                  help='the host address to listen on for http traffic')
    parser_readiness.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                                  help='the port to listen on for HTTP traffic')
    parser_readiness.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                                  help='the output format')
    parser_readiness.set_defaults(handler=readiness_handler)

    # status
    parser_status = subparsers.add_parser('status', help='see `status --help`',
                                          formatter_class=ArgumentDefaultsHelpFormatter)
    parser_status.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                               help='the host address to listen on for http traffic')
    parser_status.add_argument('--port', dest='port', default=8080, metavar='PORT', type=int,
                               help='the port to listen on for HTTP traffic')
    parser_status.add_argument('--output', dest='output', default='yaml', metavar='OUTPUT', type=str,
                               help='the output format')
    parser_status.set_defaults(handler=status_handler)

    args = parser.parse_args()
    if hasattr(args, 'handler'):
        args.handler(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
