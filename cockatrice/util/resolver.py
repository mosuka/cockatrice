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

import re
import socket


def get_ipv4(host):
    ipv4 = None

    try:
        addrlist = socket.getaddrinfo(host, None, 0, 0, 0, socket.AI_CANONNAME)
        for family, kind, proto, canonical, sockaddr in addrlist:
            if family != socket.AF_INET:
                continue
            if kind == socket.SOCK_STREAM:
                if canonical == host or canonical == '':
                    ipv4 = sockaddr[0]
                else:
                    ipv4 = get_ipv4(canonical)
            if ipv4 is not None:
                break
    except Exception as ex:
        raise ex

    return ipv4


def parse_addr(addr):
    regex = re.compile(r'''
    (                            # first capture group = Addr
      \[                         # literal open bracket                       IPv6
        [:a-fA-F0-9]+            # one or more of these characters
      \]                         # literal close bracket
      |                          # ALTERNATELY
      (?:                        #                                            IPv4
        \d{1,3}\.                # one to three digits followed by a period
      ){3}                       # ...repeated three times
      \d{1,3}                    # followed by one to three digits
      |                          # ALTERNATELY
      [-a-zA-Z0-9.]+             # one or more hostname chars ([-\w\d\.])     Hostname
    )                            # end first capture group
    (?:                          
      :                          # a literal :
      (                          # second capture group = PORT
        \d+                      # one or more digits
      )                          # end second capture group
     )?                          # ...or not.''', re.X)

    m = regex.match(addr)
    host, port = m.group(1, 2)
    try:
        return host, int(port)
    except TypeError:
        # port is None
        return host, None
