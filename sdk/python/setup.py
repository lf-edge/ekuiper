#  Copyright 2024 EMQ Technologies Co., Ltd.
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

from setuptools import setup, find_packages

setup(
    name='ekuiper',
    version='0.0.1',
    packages=find_packages(),
    url='https://github.com/lf-edge/ekuiper',
    license='Apache License 2.0',
    author='LF Edge eKuiper team',
    author_email='huangjy@emqx.io',
    description='Python SDK for eKuiper portable plugin',
    install_requires=['pynng==0.7.2'],
)
