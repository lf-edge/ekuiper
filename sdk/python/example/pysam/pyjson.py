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
import time

from ekuiper import Source, Context


class PyJson(Source):
    def __init__(self):
        self.data = {"name": "pyjson", "value": 2021}

    def configure(self, datasource: str, conf: dict):
        logging.info("configuring with datasource {} and conf {}".format(datasource, conf))

    # noinspection PyTypeChecker
    def open(self, ctx: Context):
        print("opening")
        for i in range(100):
            ctx.emit(self.data, None)
            print("emit")
            time.sleep(0.2)

    def close(self, ctx: Context):
        print("closing")
