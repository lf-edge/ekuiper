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

"""context.py: Context defines context information available during
# processing of a request.
"""
import logging
from abc import abstractmethod


class Context(object):
    """Interface defining information available at process time"""

    @abstractmethod
    def get_rule_id(self) -> str:
        """Return the ruleId of the current stream processing graph"""
        pass

    @abstractmethod
    def get_op_id(self) -> str:
        """Return the operation id"""
        pass

    @abstractmethod
    def get_instance_id(self) -> int:
        """Return the instance id"""
        pass

    @abstractmethod
    def get_logger(self) -> logging:
        """Returns the logger object that can be used to do logging"""
        pass

    @abstractmethod
    def emit(self, message: dict, meta: dict):
        """Emit the tuple to the stream"""
        pass

    @abstractmethod
    def emit_error(self, error: str):
        """Emit error to the stream"""
        pass
