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
from logging import getLogger
from threading import Thread

import yaml
from flask import Response
from werkzeug.serving import make_server


def make_response(data, output='json'):
    resp = Response()

    if output == 'json':
        resp.data = json.dumps(data, indent=2)
        resp.content_type = 'application/json; charset="UTF-8"'
    elif output == 'yaml':
        resp.data = yaml.safe_dump(data, default_flow_style=False, indent=2)
        resp.content_type = 'application/yaml; charset="UTF-8"'

    return resp


class HTTPServerThread(Thread):
    def __init__(self, host, port, app, logger=getLogger()):
        self.__logger = logger

        Thread.__init__(self)
        self.server = make_server(host, port, app)
        self.context = app.app_context()
        self.context.push()

    def run(self):
        self.server.serve_forever()

    def shutdown(self):
        self.server.shutdown()
