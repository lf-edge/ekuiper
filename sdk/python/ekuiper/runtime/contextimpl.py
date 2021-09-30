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
import sys

from .connection import SourceChannel
from .context import Context


class ContextImpl(Context):

    def __init__(self, meta: dict):
        self.ruleId = meta['ruleId']
        self.opId = meta['opId']
        self.instanceId = meta['instanceId']
        self.emitter = None

    def set_emitter(self, emitter: SourceChannel):
        self.emitter = emitter

    def get_rule_id(self) -> str:
        return self.ruleId

    def get_op_id(self) -> str:
        return self.opId

    def get_instance_id(self) -> int:
        return self.instanceId

    def get_logger(self) -> logging:
        return sys.stdout

    def emit(self, message: dict, meta: dict):
        data = {'message': message, 'meta': meta}
        json_str = json.dumps(data)
        return self.emitter.send(str.encode(json_str))

    def emit_error(self, error: str):
        data = {'error': error}
        json_str = json.dumps(data)
        return self.emitter.send(str.encode(json_str))
