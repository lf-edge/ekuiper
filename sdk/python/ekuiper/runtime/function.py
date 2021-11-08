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

import json
import logging
import traceback

from . import reg
from .connection import PairChannel
from .contextimpl import ContextImpl
from .symbol import SymbolRuntime
from ..function import Function


class FunctionRuntime(SymbolRuntime):

    def __init__(self, ctrl: dict, s: Function):
        ch = PairChannel(ctrl['symbolName'], 1)
        self.s = s
        self.ch = ch
        self.running = False
        self.key = "func_{}".format(ctrl['symbolName'])
        self.funcs = {}

    def run(self):
        self.running = True
        reg.setr(self.key, self)
        # noinspection PyBroadException
        try:
            self.ch.run(self.do_run)
        except Exception:
            if self.running:
                logging.error(traceback.format_exc())
        finally:
            self.stop()

    def do_run(self, req: bytes):
        # noinspection PyBroadException
        try:
            c = json.loads(req)
            logging.debug("running func with ", c)
            name = c['func']
            if name == "Validate":
                err = self.s.validate(c['arg'])
                if err != "":
                    return encode_reply(False, err)
                else:
                    return encode_reply(True, "")
            elif name == "Exec":
                args = c['arg']
                if isinstance(args, list) is False or len(args) < 1:
                    return encode_reply(False, 'invalid arg')
                fmeta = json.loads(args[-1])
                if 'ruleId' in fmeta and 'opId' in fmeta and 'instanceId' in fmeta \
                        and 'funcId' in fmeta:
                    key = f"{fmeta['ruleId']}_{fmeta['opId']}_{fmeta['instanceId']}" \
                          f"_{fmeta['funcId']}"
                    if key in self.funcs:
                        fctx = self.funcs[key]
                    else:
                        fctx = ContextImpl(fmeta)
                        self.funcs[key] = fctx
                else:
                    return encode_reply(False,
                                        f'invalid arg: {fmeta} ruleId, opId, instanceId and funcId'
                                        f' are required')
                r = self.s.exec(args[:-1], fctx)
                return encode_reply(True, r)
            elif name == "IsAggregate":
                r = self.s.is_aggregate()
                return encode_reply(True, r)
            else:
                return encode_reply(False, "invalid func {}".format(name))
        except Exception:
            """two occasions: normal stop will close socket to raise an error 
            OR stopped by unexpected error"""
            if self.running:
                logging.error(traceback.format_exc())
                return encode_reply(False, traceback.format_exc())

    def stop(self):
        self.running = False
        # noinspection PyBroadException
        try:
            self.ch.close()
            reg.delete(self.key)
        except Exception:
            logging.error(traceback.format_exc())

    def is_running(self) -> bool:
        return self.running


def encode_reply(state: bool, arg: str):
    return str.encode(json.dumps({'state': state, 'result': arg}))
