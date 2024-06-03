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

from . import contextimpl


class SymbolRuntime:
    """class to model the running symbol of source/sink/function"""

    @abstractmethod
    def run(self):
        """start to run the symbol"""
        pass

    @abstractmethod
    def stop(self):
        """stop the symbol"""
        pass

    @abstractmethod
    def is_running(self) -> bool:
        """check if symbol is running"""
        pass


def parse_context(ctrl):
    if ctrl['meta']['ruleId'] == "" or ctrl['meta']['opId'] == "":
        raise ('invalid arg: ', ctrl, 'ruleId and opId are required')
    return contextimpl.ContextImpl(ctrl['meta'])
