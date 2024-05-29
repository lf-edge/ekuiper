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

from typing import Any

from ekuiper import Sink, Context


class PrintSink(Sink):

    def __init__(self):
        pass

    def configure(self, conf: dict):
        print('configure print sink')

    def open(self, ctx: Context):
        print('open print sink: ', ctx)

    def collect(self, ctx: Context, data: Any):
        print('receive: ', data)

    def close(self, ctx: Context):
        print("closing print sink")
