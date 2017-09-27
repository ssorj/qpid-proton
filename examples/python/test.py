#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import sys
import subprocess
import time
import unittest

if sys.version_info[0] == 2:
    _unicode_prefix = 'u'
else:
    _unicode_prefix = ''

class ExamplesTest(unittest.TestCase):
    def test_helloworld(self, example="hello-world.py"):
        p = subprocess.Popen([example], stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                             universal_newlines=True)
        p.wait()
        output = [l.strip() for l in p.stdout]
        self.assertEqual(output, ['Hello World!'])

    def test_helloworld_direct(self):
        self.test_helloworld('hello-world-direct.py')

    def test_helloworld_blocking(self):
        self.test_helloworld('hello-world-blocking.py')

    def test_helloworld_tornado(self):
        self.test_helloworld('hello-world-tornado.py')

    def test_helloworld_direct_tornado(self):
        self.test_helloworld('hello-world-direct-tornado.py')

    def test_simple_send_recv(self, recv='simple-recv.py', send='simple-send.py'):
        r = subprocess.Popen([recv], stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                             universal_newlines=True)
        s = subprocess.Popen([send], stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                             universal_newlines=True)
        s.wait()
        r.wait()
        actual = [l.strip() for l in r.stdout]
        expected = ["{%s'sequence': int32(%i)}" % (_unicode_prefix, (i+1)) for i in range(100)]
        self.assertEqual(actual, expected)

    def test_client_server(self, client=['client.py'], server=['server.py'], sleep=0):
        s = subprocess.Popen(server, stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                             universal_newlines=True)
        if sleep:
            time.sleep(sleep)
        c = subprocess.Popen(client, stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                             universal_newlines=True)
        c.wait()
        s.terminate()
        actual = [l.strip() for l in c.stdout]
        inputs = ["Twas brillig, and the slithy toves",
                    "Did gire and gymble in the wabe.",
                    "All mimsy were the borogroves,",
                    "And the mome raths outgrabe."]
        expected = ["%s => %s" % (l, l.upper()) for l in inputs]
        self.assertEqual(actual, expected)

    def test_sync_client_server(self):
        self.test_client_server(client=['sync-client.py'])

    def test_client_server_tx(self):
        self.test_client_server(server=['server-tx.py'])

    def test_sync_client_server_tx(self):
        self.test_client_server(client=['sync-client.py'], server=['server-tx.py'])

    def test_client_server_direct(self):
        self.test_client_server(client=['client.py', '-a', 'localhost:8888/examples'], server=['server-direct.py'], sleep=0.5)

    def test_sync_client_server_direct(self):
        self.test_client_server(client=['sync-client.py', '-a', 'localhost:8888/examples'], server=['server-direct.py'], sleep=0.5)

    def test_db_send_recv(self):
        self.maxDiff = None
        # setup databases
        subprocess.check_call(['db-ctrl.py', 'init', './src_db'])
        subprocess.check_call(['db-ctrl.py', 'init', './dst_db'])
        fill = subprocess.Popen(['db-ctrl.py', 'insert', './src_db'],
                                stdin=subprocess.PIPE, stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                                universal_newlines=True)
        for i in range(100):
            fill.stdin.write("Message-%i\n" % (i+1))
        fill.stdin.close()
        fill.wait()
        # run send and recv
        r = subprocess.Popen(['db-recv.py', '-m', '100'], stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                             universal_newlines=True)
        s = subprocess.Popen(['db-send.py', '-m', '100'], stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                             universal_newlines=True)
        s.wait()
        r.wait()
        # verify output of receive
        actual = [l.strip() for l in r.stdout]
        expected = ["inserted message %i" % (i+1) for i in range(100)]
        self.assertEqual(actual, expected)
        # verify state of databases
        v = subprocess.Popen(['db-ctrl.py', 'list', './dst_db'], stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                             universal_newlines=True)
        v.wait()
        expected = ["(%i, %s'Message-%i')" % ((i+1), _unicode_prefix, (i+1)) for i in range(100)]
        actual = [l.strip() for l in v.stdout]
        self.assertEqual(actual, expected)

    def test_tx_send_tx_recv(self):
        self.test_simple_send_recv(recv='tx-recv.py', send='tx-send.py')

    def test_simple_send_direct_recv(self):
        self.maxDiff = None
        r = subprocess.Popen(['direct-recv.py', '-a', 'localhost:8888'], stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                             universal_newlines=True)
        time.sleep(0.5)
        s = subprocess.Popen(['simple-send.py', '-a', 'localhost:8888'], stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                             universal_newlines=True)
        s.wait()
        r.wait()
        actual = [l.strip() for l in r.stdout]
        expected = ["{%s'sequence': int32(%i)}" % (_unicode_prefix, (i+1)) for i in range(100)]
        self.assertEqual(actual, expected)

    def test_direct_send_simple_recv(self):
        s = subprocess.Popen(['direct-send.py', '-a', 'localhost:8888'], stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                             universal_newlines=True)
        time.sleep(0.5)
        r = subprocess.Popen(['simple-recv.py', '-a', 'localhost:8888'], stderr=subprocess.STDOUT, stdout=subprocess.PIPE,
                             universal_newlines=True)
        r.wait()
        s.wait()
        actual = [l.strip() for l in r.stdout]
        expected = ["{%s'sequence': int32(%i)}" % (_unicode_prefix, (i+1)) for i in range(100)]
        self.assertEqual(actual, expected)
