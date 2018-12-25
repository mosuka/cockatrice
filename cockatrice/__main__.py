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

import json
import os
import signal
import sys
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from logging import CRITICAL, DEBUG, ERROR, Formatter, getLogger, INFO, NOTSET, StreamHandler, WARNING
from logging.handlers import RotatingFileHandler

from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf

from cockatrice import NAME, VERSION
from cockatrice.command import add_node, delete_node, get_snapshot, get_status
from cockatrice.index_node import IndexNode


def signal_handler(signal, frame):
    sys.exit(0)


def server_handler(args):
    # create logger and handler
    logger = getLogger(NAME)
    log_handler = StreamHandler()

    # determine log destination
    if args.log_file is not None:
        log_handler = RotatingFileHandler(args.log_file, 'a+', maxBytes=args.log_file_max_bytes,
                                          backupCount=args.log_file_backup_count)

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
        http_log_handler = RotatingFileHandler(args.http_log_file, 'a+', maxBytes=args.http_log_file_max_bytes,
                                               backupCount=args.http_log_file_backup_count)

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
    os.makedirs(os.path.dirname(args.snapshot_file), exist_ok=True)
    conf = SyncObjConf()
    conf.fullDumpFile = args.snapshot_file
    conf.logCompactionMinEntries = args.log_compaction_min_entries
    conf.logCompactionMinTime = args.log_compaction_min_time
    conf.dynamicMembershipChange = True
    conf.validate()

    index_node = None
    try:
        index_node = IndexNode(host=args.host, port=args.port, seed_addr=args.seed_addr, conf=conf,
                               index_dir=args.index_dir, http_port=args.http_port, logger=logger,
                               http_logger=http_logger, metrics_registry=metrics_registry)
        while True:
            signal.pause()
    except Exception as ex:
        print(ex)
    finally:
        if index_node is not None:
            index_node.stop()


def status_handler(args):
    print(json.dumps(get_status(bind_addr=args.bind_addr, timeout=0.5)))


def join_handler(args):
    print(json.dumps(add_node(args.join_addr, bind_addr=args.bind_addr, timeout=0.5)))


def leave_handler(args):
    print(json.dumps(delete_node(args.leave_addr, bind_addr=args.bind_addr, timeout=0.5)))


def snapshot_handler(args):
    snapshot = get_snapshot(bind_addr=args.bind_addr, timeout=0.5)
    if args.output_file is None:
        print(snapshot)
    else:
        with open(args.output_file, 'wb') as f:
            f.write(snapshot)


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGQUIT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = ArgumentParser(description='cockatrice command', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--version', action='version', version='cockatrice {0}'.format(VERSION))

    subparsers = parser.add_subparsers()

    # server
    parser_server = subparsers.add_parser('server', help='see `server --help`',
                                          formatter_class=ArgumentDefaultsHelpFormatter)
    parser_server.add_argument('--host', dest='host', default='localhost', metavar='HOST', type=str,
                               help='the host address to listen on for peer traffic')
    parser_server.add_argument('--port', dest='port', default=7070, metavar='PORT', type=int,
                               help='the port to listen on for peer traffic')
    parser_server.add_argument('--seed-addr', dest='seed_addr', default=None, metavar='SEED_ADDR', type=str,
                               help='the address of the node in the existing cluster')
    parser_server.add_argument('--snapshot-file', dest='snapshot_file', default='/tmp/cockatrice/snapshot.zip',
                               metavar='SNAPSHOT_FILE', type=str, help='file to store snapshot of all indices')
    parser_server.add_argument('--log-compaction-min-entries', dest='log_compaction_min_entries', default=5000,
                               metavar='LOG_COMPACTION_MIN_ENTRIES', type=int,
                               help='log-compaction interval min entries')
    parser_server.add_argument('--log-compaction-min-time', dest='log_compaction_min_time', default=300,
                               metavar='LOG_COMPACTION_MIN_TIME', type=int,
                               help='log-compaction interval min time in seconds')
    parser_server.add_argument('--index-dir', dest='index_dir', default='/tmp/cockatrice/index', metavar='INDEX_DIR',
                               type=str, help='index dir')
    parser_server.add_argument('--http-port', dest='http_port', default=8080,
                               metavar='HTTP_PORT', type=int, help='the port to listen on for HTTP traffic')
    parser_server.add_argument('--log-level', dest='log_level', default='DEBUG', metavar='LOG_LEVEL', type=str,
                               help='log level')
    parser_server.add_argument('--log-file', dest='log_file', default=None, metavar='LOG_FILE', type=str,
                               help='log file')
    parser_server.add_argument('--log-file-max-bytes', dest='log_file_max_bytes', default=512000000,
                               metavar='LOG_FILE_MAX_BYTES', type=int, help='log file max bytes')
    parser_server.add_argument('--log-file-backup-count', dest='log_file_backup_count', default=5,
                               metavar='LOG_FILE_BACKUP_COUNT', type=int, help='log file backup count')
    parser_server.add_argument('--http-log-file', dest='http_log_file', default=None, metavar='HTTP_LOG_FILE', type=str,
                               help='http log file')
    parser_server.add_argument('--http-log-file-max-bytes', dest='http_log_file_max_bytes', default=512000000,
                               metavar='HTTP_LOG_FILE_MAX_BYTES', type=int, help='http log file max bytes')
    parser_server.add_argument('--http-log-file-backup-count', dest='http_log_file_backup_count', default=5,
                               metavar='HTTP_LOG_FILE_BACKUP_COUNT', type=int, help='http log file backup count')
    parser_server.set_defaults(handler=server_handler)

    # status
    parser_status = subparsers.add_parser('status', help='see `status --help`',
                                          formatter_class=ArgumentDefaultsHelpFormatter)
    parser_status.add_argument('--bind-addr', dest='bind_addr', default='localhost:7070', metavar='BIND_ADDR', type=str,
                               help='the address to listen on for peer traffic')
    parser_status.set_defaults(handler=status_handler)

    # join
    parser_join = subparsers.add_parser('join', help='see `join --help`', formatter_class=ArgumentDefaultsHelpFormatter)
    parser_join.add_argument('--bind-addr', dest='bind_addr', default='localhost:7070', metavar='BIND_ADDR', type=str,
                             help='the address to listen on for peer traffic')
    parser_join.add_argument('--join-addr', dest='join_addr', default=None, metavar='JOIN_ADDR', type=str,
                             help='the address of node to join to the cluster')
    parser_join.set_defaults(handler=join_handler)

    # leave
    parser_leave = subparsers.add_parser('leave', help='see `leave --help`',
                                         formatter_class=ArgumentDefaultsHelpFormatter)
    parser_leave.add_argument('--bind-addr', dest='bind_addr', default='localhost:7070',
                              metavar='BIND_ADDR', type=str, help='the address to listen on for peer traffic')
    parser_leave.add_argument('--leave-addr', dest='leave_addr', default=None, metavar='LEAVE_ADDR', type=str,
                              help='the address of node to leave from the cluster')
    parser_leave.set_defaults(handler=leave_handler)

    # snapshot
    parser_snapshot = subparsers.add_parser('snapshot', help='see `snapshot --help`',
                                            formatter_class=ArgumentDefaultsHelpFormatter)
    parser_snapshot.add_argument('--bind-addr', dest='bind_addr', default='localhost:7070',
                                 metavar='BIND_ADDR', type=str, help='the address to listen on for peer traffic')
    parser_snapshot.add_argument('--output-file', dest='output_file', default=None, metavar='OUTPUT_FILE', type=str,
                                 help='the snapshot file destination')
    parser_snapshot.set_defaults(handler=snapshot_handler)

    args = parser.parse_args()
    if hasattr(args, 'handler'):
        args.handler(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
