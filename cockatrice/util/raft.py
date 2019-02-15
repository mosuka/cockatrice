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

import functools
import os
import socket
from contextlib import closing

from pysyncobj import _RAFT_STATE, FAIL_REASON, SyncObj
from pysyncobj.encryptor import getEncryptor
from pysyncobj.poller import createPoller
from pysyncobj.tcp_connection import TcpConnection

from cockatrice.util.resolver import get_ipv4, parse_addr

RAFT_DATA_FILE = 'raft.bin'


class RaftNode(SyncObj):
    def __init__(self, selfNodeAddr, otherNodesAddrs, conf=None, consumers=None, metadata=None):
        self.__selfNodeAddr = selfNodeAddr
        self.__otherNodesAddrs = otherNodesAddrs
        self.__conf = conf
        self.__consumers = consumers

        self.__metadata = metadata

        super(RaftNode, self).__init__(self.__selfNodeAddr, self.__otherNodesAddrs, conf=self.__conf,
                                       consumers=self.__consumers)

    # override SyncObj.__utilityCallback
    def _SyncObj__utilityCallback(self, res, err, conn, cmd, node):
        cmdResult = 'FAIL'
        if err == FAIL_REASON.SUCCESS:
            cmdResult = 'SUCCESS'
        conn.send(cmdResult + ' ' + cmd + ' ' + node)

    # override SyncObj.__onUtilityMessage
    def _SyncObj__onUtilityMessage(self, conn, message):
        try:
            if message[0] == 'status':
                conn.send(self.getStatus())
                return True
            elif message[0] == 'add':
                self.addNodeToCluster(message[1],
                                      callback=functools.partial(self._SyncObj__utilityCallback, conn=conn, cmd='ADD',
                                                                 node=message[1]))
                return True
            elif message[0] == 'remove':
                if message[1] == self.__selfNodeAddr:
                    conn.send('FAIL REMOVE ' + message[1])
                else:
                    self.removeNodeFromCluster(message[1],
                                               callback=functools.partial(self._SyncObj__utilityCallback, conn=conn,
                                                                          cmd='REMOVE', node=message[1]))
                return True
            elif message[0] == 'set_version':
                self.setCodeVersion(message[1],
                                    callback=functools.partial(self._SyncObj__utilityCallback, conn=conn,
                                                               cmd='SET_VERSION', node=str(message[1])))
                return True
            # elif message[0] == 'get_snapshot':
            #     if os.path.exists(self.__conf.fullDumpFile):
            #         with open(self.__conf.fullDumpFile, 'rb') as f:
            #             data = f.read()
            #             conn.send(data)
            #     else:
            #         conn.send(b'')
            #     return True
            elif message[0] == 'get_leader':
                conn.send(self.getStatus()['leader'])
                return True
            elif message[0] == 'get_peers':
                conn.send(self.getStatus()['peers'])
                return True
            elif message[0] == 'get_metadata':
                conn.send(self.getMetadata())
                return True
            elif message[0] == 'is_healthy':
                conn.send(str(self.isHealthy()))
                return True
            elif message[0] == 'is_alive':
                conn.send(str(self.isAlive()))
                return True
            elif message[0] == 'is_ready':
                conn.send(str(self.isReady()))
                return True
        except Exception as e:
            conn.send(str(e))
            return True

    def isHealthy(self):
        return self.isAlive() and self.isReady()

    def isAlive(self):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            return sock.connect_ex(parse_addr(self._getSelfNodeAddr())) == 0

    def getMetadata(self):
        return self.__metadata

    def getStatus(self):
        status = super().getStatus()

        status['others'] = [n[len('partner_node_status_server_'):] for n in status.keys() if
                            n.startswith('partner_node_status_server_')]

        status['peers'] = list(status['others'])
        status['peers'].append(status['self'])

        if status['state'] == _RAFT_STATE.FOLLOWER:
            status['state_str'] = 'follower'
        elif status['state'] == _RAFT_STATE.CANDIDATE:
            status['state_str'] = 'candidate'
        elif status['state'] == _RAFT_STATE.LEADER:
            status['state_str'] = 'leader'

        metadata = {status['self']: self.getMetadata()}
        for other in status['others']:
            metadata.update({other: get_metadata(other)})
        status['metadata'] = metadata

        healthiness = {status['self']: self.isHealthy()}
        for other in status['others']:
            healthiness.update({other: is_healthy(other)})
        status['healthiness'] = healthiness

        return status


class RaftCommand:
    def __init__(self, cmd, args=None, bind_addr='localhost:7070', password=None, timeout=10):
        try:
            self.__result = None

            self.__request = [cmd]
            if args is not None:
                self.__request.extend(args)

            host, port = parse_addr(bind_addr)

            self.__host = get_ipv4(host)
            self.__port = int(port)
            self.__password = password
            self.__timeout = timeout
            self.__poller = createPoller('auto')
            self.__connection = TcpConnection(self.__poller, onMessageReceived=self.__on_message_received,
                                              onConnected=self.__on_connected, onDisconnected=self.__on_disconnected,
                                              socket=None, timeout=self.__timeout, sendBufferSize=2 ** 16,
                                              recvBufferSize=2 ** 16)
            if self.__password is not None:
                self.__connection.encryptor = getEncryptor(self.__password)
            self.__is_connected = self.__connection.connect(self.__host, self.__port)
            while self.__is_connected:
                self.__poller.poll(self.__timeout)
        except Exception as ex:
            raise ex

    def __on_message_received(self, message):
        if self.__connection.encryptor and not self.__connection.sendRandKey:
            self.__connection.sendRandKey = message
            self.__connection.send(self.__request)
            return

        self.__result = message
        self.__connection.disconnect()

    def __on_connected(self):
        if self.__connection.encryptor:
            self.__connection.recvRandKey = os.urandom(32)
            self.__connection.send(self.__connection.recvRandKey)
            return

        self.__connection.send(self.__request)

    def __on_disconnected(self):
        self.__is_connected = False

    def get_result(self):
        return self.__result


def execute(cmd, args=None, bind_addr='127.0.0.1:7070', password=None, timeout=10):
    try:
        response = RaftCommand(cmd, args=args, bind_addr=bind_addr, password=password,
                               timeout=timeout).get_result()
    except Exception as ex:
        raise ex

    return response


def get_status(bind_addr='127.0.0.1:7070', password=None, timeout=10):
    return execute('status', args=None, bind_addr=bind_addr, password=password, timeout=timeout)


def add_node(node_name, bind_addr='127.0.0.1:7070', password=None, timeout=10):
    return execute('add', args=[node_name], bind_addr=bind_addr, password=password, timeout=timeout)


def delete_node(node_name, bind_addr='127.0.0.1:7070', password=None, timeout=10):
    return execute('remove', args=[node_name], bind_addr=bind_addr, password=password, timeout=timeout)


def get_snapshot(bind_addr='127.0.0.1:7070', password=None, timeout=10):
    return execute('get_snapshot', args=None, bind_addr=bind_addr, password=password, timeout=timeout)


def get_leader(bind_addr='127.0.0.1:7070', password=None, timeout=10):
    return execute('get_leader', args=None, bind_addr=bind_addr, password=password, timeout=timeout)


def get_peers(bind_addr='127.0.0.1:7070', password=None, timeout=10):
    return execute('get_peers', args=None, bind_addr=bind_addr, password=password, timeout=timeout)


def get_metadata(bind_addr='127.0.0.1:7070', password=None, timeout=10):
    return execute('get_metadata', args=None, bind_addr=bind_addr, password=password, timeout=timeout)


def is_healthy(bind_addr='127.0.0.1:7070', password=None, timeout=10):
    return execute('is_healthy', args=None, bind_addr=bind_addr, password=password, timeout=timeout) == 'True'


def is_alive(bind_addr='127.0.0.1:7070', password=None, timeout=10):
    return execute('is_alive', args=None, bind_addr=bind_addr, password=password, timeout=timeout) == 'True'


def is_ready(bind_addr='127.0.0.1:7070', password=None, timeout=10):
    return execute('is_ready', args=None, bind_addr=bind_addr, password=password, timeout=timeout) == 'True'
