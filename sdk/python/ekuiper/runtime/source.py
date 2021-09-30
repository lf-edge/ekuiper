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
import traceback

from . import reg
from .connection import SourceChannel
from .symbol import parse_context, SymbolRuntime
from ..source import Source


class SourceRuntime(SymbolRuntime):

    def __init__(self, ctrl: dict, s: Source):
        ctx = parse_context(ctrl)
        ds = ""
        config = {}
        if 'datasource' in ctrl:
            ds = ctrl['datasource']
        if 'config' in ctrl:
            config = ctrl['config']
        s.configure(ds, config)
        ch = SourceChannel(ctrl['meta'])
        ctx.set_emitter(ch)
        key = f"{ctrl['meta']['ruleId']}_{ctrl['meta']['opId']}" \
              f"_{ctrl['meta']['instanceId']}_{ctrl['symbolName']}"
        self.s = s
        self.ctx = ctx
        self.ch = ch
        self.running = False
        self.key = key

    def run(self):
        logging.info('start running source')
        self.running = True
        reg.setr(self.key, self)
        # noinspection PyBroadException
        try:
            self.s.open(self.ctx)
        except Exception:
            """two occasions: normal stop will close socket to raise an error OR\
             stopped by unexpected error"""
            if self.running:
                logging.error(traceback.format_exc())
        finally:
            self.stop()

    def stop(self):
        self.running = False
        # noinspection PyBroadException
        try:
            self.s.close(self.ctx)
            self.ch.close()
            reg.delete(self.key)
        except Exception:
            logging.error(traceback.format_exc())

    def is_running(self) -> bool:
        return self.running
