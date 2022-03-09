#  Copyright 2021 EMQ Technologies Co., Ltd.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import logging
import time
from typing import Callable

from pynng import Req0, Push0, Pull0, Timeout


class PairChannel:

    def __init__(self, name: str, typ: int):
        s = Req0(resend_time=0)
        """TODO options"""
        if typ == 0:
            url = "ipc:///tmp/plugin_{}.ipc".format(name)
        else:
            url = "ipc:///tmp/func_{}.ipc".format(name)
        try:
            dial_with_retry(s, url)
        except Exception as e:
            print(e)
            exit(0)
        self.sock = s

    """ run this in a new thread"""

    def run(self, reply_func: Callable[[bytes], bytes]):
        self.sock.send(b'handshake')
        while True:
            try:
                msg = self.sock.recv()
                reply = reply_func(msg)
                self.sock.send(reply)
            except Timeout:
                print('pair timeout')
                pass

    def close(self):
        self.sock.close()


class SourceChannel:

    def __init__(self, meta: dict):
        s = Push0()
        url = "ipc:///tmp/{}_{}_{}.ipc".format(meta['ruleId'], meta['opId'], meta['instanceId'])
        logging.info(url)
        dial_with_retry(s, url)
        self.sock = s

    def send(self, data: bytes):
        self.sock.send(data)

    def close(self):
        self.sock.close()


class SinkChannel:

    def __init__(self, meta: dict):
        s = Pull0()
        url = "ipc:///tmp/{}_{}_{}.ipc".format(meta['ruleId'], meta['opId'], meta['instanceId'])
        logging.info(url)
        listen_with_retry(s, url)
        self.sock = s

    def recv(self) -> bytes:
        return self.sock.recv()

    def close(self):
        self.sock.close()


def listen_with_retry(sock, url: str):
    retry_count = 10
    retry_interval = 0.05
    while True:
        # noinspection PyBroadException
        try:
            sock.listen(url)
            break
        except Exception:
            retry_count -= 1
            if retry_count < 0:
                raise
        time.sleep(retry_interval)


def dial_with_retry(sock, url: str):
    retry_count = 50
    retry_interval = 0.1
    while True:
        try:
            sock.dial(url, block=True)
            break
        except Exception:
            retry_count -= 1
            if retry_count < 0:
                raise
        time.sleep(retry_interval)
