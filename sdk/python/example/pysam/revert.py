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

from typing import Any, List

from ekuiper import Function, Context


class RevertFunc(Function):

    def __init__(self):
        pass

    def validate(self, args: List[Any]):
        return ""

    def exec(self, args: List[Any], ctx: Context):
        return args[0][::-1]

    def is_aggregate(self):
        return False


revertIns = RevertFunc()
