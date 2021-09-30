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

from .runtime.context import Context


class Source(object):
    """abstract class for eKuiper source plugin"""

    @abstractmethod
    def configure(self, datasource: str, conf: dict):
        """configure with the string datasource and conf map and raise error if any"""
        pass

    @abstractmethod
    def open(self, ctx: Context):
        """run continuously and send out the data or error with ctx"""
        pass

    @abstractmethod
    def close(self, ctx: Context):
        """stop running and clean up"""
        pass
