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
import json

from threading import Thread
from argparse import ArgumentParser
from logging import getLogger, StreamHandler, Formatter, CRITICAL, ERROR, WARNING, INFO, DEBUG, NOTSET
from logging.handlers import RotatingFileHandler

from prometheus_client.core import CollectorRegistry
from pysyncobj import SyncObjConf

from cockatrice import NAME, VERSION
from cockatrice.index_node import IndexNode
from cockatrice.command import execute


def signal_handler(signal, frame):
    sys.exit(0)


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

    index_node = None
    try:
        if args.seed_addr is not None:
            # clear the peer addresses due to update peer addresses from the cluster.
            args.peer_addrs.clear()

            # execute a command to get status from the cluster
            status_result = execute('status', bind_addr=args.seed_addr, timeout=0.5)
            if status_result is None:
                raise ValueError('command execution failed to {0}'.format(args.seed_addr))

            # get peer addresses from above command result
            self_addr = status_result['data']['self']
            if self_addr not in args.peer_addrs:
                args.peer_addrs.append(self_addr)
            for k in status_result['data'].keys():
                if k.startswith('partner_node_status_server_'):
                    partner_addr = k[len('partner_node_status_server_'):]
                    if partner_addr not in args.peer_addrs:
                        args.peer_addrs.append(partner_addr)

            if args.bind_addr not in args.peer_addrs:
                Thread(target=execute, kwargs={'cmd': 'add', 'args': [args.bind_addr], 'bind_addr': args.seed_addr,
                                               'timeout': 0.5, 'logger': logger}).start()

            # remove this node's address from peer addresses.
            if args.bind_addr in args.peer_addrs:
                args.peer_addrs.remove(args.bind_addr)

        index_node = IndexNode(args.bind_addr, args.peer_addrs, conf=conf, index_dir=args.index_dir,
                               http_port=args.http_port, logger=logger, http_logger=http_logger,
                               metrics_registry=metrics_registry)
        index_node.start()
    except ValueError as ex:
        print(ex)
    finally:
        if index_node is not None:
            index_node.stop()


def status_handler(args):
    print(json.dumps(execute('status', bind_addr=args.bind_addr, timeout=0.5)))


def join_handler(args):
    print(json.dumps(execute('add', args=[args.join_addr], bind_addr=args.bind_addr, timeout=0.5)))


def leave_handler(args):
    print(json.dumps(execute('remove', args=[args.leave_addr], bind_addr=args.bind_addr, timeout=0.5)))


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGQUIT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = ArgumentParser(description='cockatrice command')
    parser.add_argument('-v', '--version', action='version', version='cockatrice {}'.format(VERSION))

    subparsers = parser.add_subparsers()

    parser_server = subparsers.add_parser('server', help='see `server --help`')
    parser_server.add_argument('--http-port', dest='http_port', default=8080, metavar='HTTP_PORT', type=int,
                               help='http port')
    parser_server.add_argument('--bind-addr', dest='bind_addr', default='127.0.0.1:7070', metavar='BIND_ADDR',
                               type=str, help='the address to listen on for peer traffic')
    parser_server.add_argument('--seed-addr', dest='seed_addr', default=None, metavar='SEED_ADDR',
                               type=str, help='the address of the node in the existing cluster')
    parser_server.add_argument('--peer-addr', dest='peer_addrs', default=[], action='append', metavar='PEER_ADDR',
                               type=str, help='the address of the peer node in the cluster')
    parser_server.add_argument('--index-dir', dest='index_dir', default='/tmp/cockatrice/index', metavar='INDEX_DIR',
                               type=str, help='index dir')
    parser_server.add_argument('--dump-file', dest='dump_file', default='/tmp/cockatrice/raft/data.dump',
                               metavar='DUMP_FILE', type=str, help='dump file')
    parser_server.add_argument('--log-level', dest='log_level', default='DEBUG', metavar='LOG_LEVEL', type=str,
                               help='log level')
    parser_server.add_argument('--log-file', dest='log_file', default=None, metavar='LOG_FILE', type=str,
                               help='log file')
    parser_server.add_argument('--http-log-file', dest='http_log_file', default=None, metavar='HTTP_LOG_FILE', type=str,
                               help='http log file')
    parser_server.set_defaults(handler=server_handler)

    parser_status = subparsers.add_parser('status', help='see `status --help`')
    parser_status.add_argument('--bind-addr', dest='bind_addr', default='127.0.0.1:7070', metavar='BIND_ADDR',
                               type=str, help='the address to listen on for peer traffic')
    parser_status.set_defaults(handler=status_handler)

    parser_join = subparsers.add_parser('join', help='see `join --help`')
    parser_join.add_argument('--bind-addr', dest='bind_addr', default='127.0.0.1:7070', metavar='BIND_ADDR',
                             type=str, help='the address to listen on for peer traffic')
    parser_join.add_argument('--join-addr', dest='join_addr', default=None, metavar='JOIN_ADDR', type=str,
                             help='the address of node to join to the cluster')
    parser_join.set_defaults(handler=join_handler)

    parser_join = subparsers.add_parser('leave', help='see `leave --help`')
    parser_join.add_argument('--bind-addr', dest='bind_addr', default='127.0.0.1:7070', metavar='BIND_ADDR',
                             type=str, help='the address to listen on for peer traffic')
    parser_join.add_argument('--leave-addr', dest='leave_addr', default=None, metavar='LEAVE_ADDR', type=str,
                             help='the address of node to leave from the cluster')
    parser_join.set_defaults(handler=leave_handler)

    args = parser.parse_args()
    if hasattr(args, 'handler'):
        args.handler(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
