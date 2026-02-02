#!/bin/sh
#
# Copyright 2021-2024 EMQ Technologies Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

dir=/usr/local/onnx

OS=$(uname -s)
ARCH=$(uname -m)

cur=$(dirname "$0")
echo "Base path $cur"
if [ -d "$dir" ]; then
  echo "SDK path $dir exists."
else
  echo "Creating SDK path $dir"
  mkdir -p $dir/lib
  echo "Created SDK path $dir"
  echo "Moving libs"

  if [ "$OS" = "Darwin" ]; then
      if [ "$ARCH" = "arm64" ]; then
          cp -R "$cur"/lib/onnxruntime_arm64.dylib $dir/lib
      else
        echo "Unable to determine a path to the onnxruntime shared library for OS \"$OS\" and architecture \"$ARCH\"."
      fi
  elif [ "$OS" = "Linux" ]; then
      if [ "$ARCH" = "aarch64" ]; then
          cp -R "$cur"/lib/onnxruntime_arm64.so $dir/lib
      else
          cp -R "$cur"/lib/onnxruntime.so $dir/lib
      fi
  else
      echo "Unable to determine a path to the onnxruntime shared library for OS \"$OS\" and architecture \"$ARCH\"."
  fi
  echo "Moved libs"
fi

if [ -f "/etc/ld.so.conf.d/onnx.conf" ]; then
  echo "/etc/ld.so.conf.d/onnx.conf exists"
else
  echo "Copy conf file"
  cp "$cur"/onnx.conf /etc/ld.so.conf.d/
  echo "Copied conf file"
fi
ldconfig
