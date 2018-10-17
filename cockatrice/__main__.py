#!/usr/bin/env python

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
import sys
import signal

from argparse import ArgumentParser
from logging import getLogger, StreamHandler, Formatter, CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET
from logging.handlers import RotatingFileHandler

from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf

from cockatrice import NAME, VERSION
from cockatrice.data_node import DataNode
from cockatrice.http_server import HTTPServer


def signal_handler(signal, frame):
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


def server_handler(args):
    # create logger and handler
    logger = getLogger(NAME)
    log_handler = StreamHandler()

    # determine log destination
    if args.log_file is not None:
        log_handler = RotatingFileHandler(args.log_file, 'a+', maxBytes=512000000, backupCount=5)

    # determine log level
    if args.log_level in ['CRITICAL', 'FATAL']:
        logger.setLevel(CRITICAL)
        log_handler.setLevel(CRITICAL)
    elif args.log_level == 'ERROR':
        logger.setLevel(ERROR)
        log_handler.setLevel(ERROR)
    elif args.log_level in ['WARNING', 'WARN']:
        logger.setLevel(WARNING)
        log_handler.setLevel(WARNING)
    elif args.log_level == 'INFO':
        logger.setLevel(INFO)
        log_handler.setLevel(INFO)
    elif args.log_level == 'DEBUG':
        logger.setLevel(DEBUG)
        log_handler.setLevel(DEBUG)
    elif args.log_level == 'NOTSET':
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
    if args.http_log_file is not None:
        http_log_handler = RotatingFileHandler(args.http_log_file, 'a+', maxBytes=512000000, backupCount=5)

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

    if args.dump_file is not None:
        os.makedirs(os.path.dirname(args.dump_file), exist_ok=True)

    conf = SyncObjConf(
        fullDumpFile=args.dump_file,
        logCompactionMinTime=300,
        dynamicMembershipChange=True
    )

    data_node = DataNode(args.bind_addr, args.peer_addr, conf, args.index_dir, logger)
    http_server = HTTPServer(NAME, args.http_port, data_node, logger, http_logger, metrics_registry)
    http_server.start()


def client_handler(args):
    print(__name__)


def main():
    parser = ArgumentParser(description='cockatrice command')
    parser.add_argument('-v', '--version', action='version', version='cockatrice {}'.format(VERSION))

    subparsers = parser.add_subparsers()

    parser_server = subparsers.add_parser('server', help='see `server --help`')
    parser_server.add_argument('--http-port', dest='http_port', default=8080, metavar='HTTP_PORT', type=int,
                               help='http port')
    parser_server.add_argument('--bind-addr', dest='bind_addr', default='127.0.0.1:7070', metavar='BIND_ADDR',
                               type=str, help='host address')
    parser_server.add_argument('--peer-addr', dest='peer_addr', default=[], action='append', metavar='PEER_ADDR',
                               type=str, help='peer address')
    parser_server.add_argument('--index-dir', dest='index_dir', default='/tmp/cockatrice/index', metavar='INDEX_DIR',
                               type=str, help='index dir')
    # parser_server.add_argument('--schema-file', dest='schema_file', default=None, metavar='SCHEMA_FILE', type=str,
    #                            help='schema file')
    parser_server.add_argument('--dump-file', dest='dump_file', default='/tmp/cockatrice/raft/data.dump',
                               metavar='DUMP_FILE', type=str, help='dump file')
    parser_server.add_argument('--log-level', dest='log_level', default='DEBUG', metavar='LOG_LEVEL', type=str,
                               help='log level')
    parser_server.add_argument('--log-file', dest='log_file', default=None, metavar='LOG_FILE', type=str,
                               help='log file')
    parser_server.add_argument('--http-log-file', dest='http_log_file', default=None, metavar='HTTP_LOG_FILE', type=str,
                               help='http log file')
    parser_server.set_defaults(handler=server_handler)

    parser_client = subparsers.add_parser('client', help='see `client --help`')
    parser_client.add_argument('-H', metavar='HOST', help='host address')
    parser_client.set_defaults(handler=client_handler)

    args = parser.parse_args()
    if hasattr(args, 'handler'):
        args.handler(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
