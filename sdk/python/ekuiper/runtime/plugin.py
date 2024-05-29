#  Copyright 2021-2023 EMQ Technologies Co., Ltd.
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
import sys
import threading
import traceback
from typing import Dict, Callable

from . import reg, shared
from .connection import PairChannel
from .function import FunctionRuntime
from .sink import SinkRuntime
from .source import SourceRuntime
from ..function import Function
from ..sink import Sink
from ..source import Source


class PluginConfig:

    def __init__(self, name: str, sources: Dict[str, Callable[[], Source]],
                 sinks: Dict[str, Callable[[], Sink]],
                 functions: Dict[str, Callable[[], Function]]):
        self.name = name
        self.sources = sources
        self.sinks = sinks
        self.functions = functions

    def get(self, plugin_type: str, symbol_name: str):
        if plugin_type == shared.TYPE_SOURCE:
            return self.sources[symbol_name]
        elif plugin_type == shared.TYPE_SINK:
            return self.sinks[symbol_name]
        elif plugin_type == shared.TYPE_FUNC:
            return self.functions[symbol_name]
        else:
            return None


conf: PluginConfig


def start(c: PluginConfig):
    init_vars(c)
    global conf
    conf = c
    logging.info("starting plugin {}".format(c.name))
    ch = PairChannel(c.name, 0)
    ch.run(command_reply)
    logging.info("started plugin {}".format(c.name))


def init_vars(c: PluginConfig):
    # if len(sys.argv) != 2:
    #     msg = gettext('fail to init plugin, must pass exactly 2 args but got {}'.format(sys.argv))
    #     raise ValueError(msg)
    # """TODO validation"""
    # arg = json.loads(sys.argv[1])
    # noinspection PyTypeChecker
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(pathname)s[line:%(lineno)d]'
                                  ' - %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)


# noinspection PyTypeChecker
def command_reply(req: bytes) -> bytes:
    # noinspection PyBroadException
    try:
        cmd = json.loads(req)
        logging.debug("receive command {}".format(cmd))
        ctrl = json.loads(cmd['arg'])
        logging.debug(ctrl)
        if cmd['cmd'] == shared.CMD_START:
            f = conf.get(ctrl['pluginType'], ctrl['symbolName'])
            if f is None:
                return b'symbol not found'
            s = f()
            if ctrl['pluginType'] == shared.TYPE_SOURCE:
                logging.info("running source {}".format(ctrl['symbolName']))
                runtime = SourceRuntime(ctrl, s)
                x = threading.Thread(target=runtime.run, daemon=True)
                x.start()
            elif ctrl['pluginType'] == shared.TYPE_SINK:
                logging.info("running sink {}".format(ctrl['symbolName']))
                # noinspection PyTypeChecker
                runtime = SinkRuntime(ctrl, s)
                x = threading.Thread(target=runtime.run, daemon=True)
                x.start()
            elif ctrl['pluginType'] == shared.TYPE_FUNC:
                logging.info("running function {}".format(ctrl['symbolName']))
                runtime = FunctionRuntime(ctrl, s)
                x = threading.Thread(target=runtime.run, daemon=True)
                x.start()
            else:
                return b'invalid plugin type'
        elif cmd['cmd'] == shared.CMD_STOP:
            regkey = f"{ctrl['meta']['ruleId']}_{ctrl['meta']['opId']}" \
                     f"_{ctrl['meta']['instanceId']}_{ctrl['symbolName']}"
            logging.info("stopping {}".format(regkey))
            if reg.has(regkey):
                runtime = reg.get(regkey)
                if runtime.is_running():
                    runtime.stop()
            else:
                logging.warning("symbol ", regkey, " not found")
        return b'ok'
    except Exception:
        var = traceback.format_exc()
        return str.encode(var)
