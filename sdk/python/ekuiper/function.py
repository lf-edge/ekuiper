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
from typing import List, Any

from .runtime.context import Context


class Function(object):
    """abstract class for eKuiper function plugin"""

    @abstractmethod
    def validate(self, args: List[Any]):
        """callback to validate against ast args, return a string error or empty string"""
        pass

    @abstractmethod
    def exec(self, args: List[Any], ctx: Context) -> Any:
        """callback to do execution, return result"""
        pass

    @abstractmethod
    def is_aggregate(self):
        """callback to check if function is for aggregation, return bool"""
        pass
