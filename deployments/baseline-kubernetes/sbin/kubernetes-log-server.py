#!/usr/bin/env python3

#  Copyright 2022 Bytedance Inc.
#
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

import argparse
import subprocess
import textwrap

parser = argparse.ArgumentParser(
    description=textwrap.dedent('''\
        A simple HTTP server for Kubernetes container logs. (not recommended for production)
        Usages:
          - python3 kubernetes-log-server.py
          - python3 kubernetes-log-server.py --port <PORT>
          - python3 kubernetes-log-server.py --host <HOST> --port <PORT>
        '''))
parser.add_argument('--host', dest='host', type=str,
                    default='',
                    help='host of the server')
parser.add_argument('--port', dest='port', type=int,
                    default='8080',
                    help='port of the server')


class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):
        # Parse query params
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        print('query params: {}'.format(params))

        # Fetch logs
        logs = get_kubernetes_container_logs(
            params.get('namespace', ['default'])[0],
            params.get('pod')[0],
            params.get('container', [None])[0])

        # Assemble response
        self.send_response(200)
        self.send_header("Content-type", "text")
        self.end_headers()
        self.wfile.write(logs)


def get_kubernetes_container_logs(namespace, pod, container=None):
    cmd = ['kubectl', '-n', namespace, 'logs', pod]
    if container is not None:
        cmd += ['-c', container]

    return subprocess \
        .run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT) \
        .stdout


if __name__ == "__main__":
    # Parse CLI arguments
    args = parser.parse_args()
    host = args.host
    port = args.port

    # Start the server
    server = HTTPServer((host, port), MyServer)
    print("Server started at http://%s:%s" % (host, port))

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass

    server.server_close()
    print("Server stopped.")
