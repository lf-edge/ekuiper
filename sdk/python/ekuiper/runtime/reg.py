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
from typing import Dict

from .symbol import SymbolRuntime

runtimes: Dict[str, SymbolRuntime] = {}


def has(name: str) -> bool:
    return name in runtimes


def get(name: str) -> SymbolRuntime:
    return runtimes[name]


def setr(name: str, r: SymbolRuntime):
    logging.info("set {}".format(name))
    runtimes[name] = r


def delete(name: str):
    # noinspection PyBroadException
    logging.info("delete {}".format(name))
    try:
        del runtimes[name]
    except Exception:
        logging.error(traceback.format_exc())
