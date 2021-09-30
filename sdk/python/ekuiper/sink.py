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

from abc import abstractmethod
from typing import Any

from .runtime.context import Context


class Sink(object):
    """abstract class for eKuiper sink plugin"""

    @abstractmethod
    def configure(self, conf: dict):
        """configure with conf map and raise error if any"""
        pass

    @abstractmethod
    def open(self, ctx: Context):
        """open connection and wait to receive data"""
        pass

    @abstractmethod
    def collect(self, ctx: Context, data: Any):
        """callback to deal with received data"""
        pass

    @abstractmethod
    def close(self, ctx: Context):
        """stop running and clean up"""
        pass
