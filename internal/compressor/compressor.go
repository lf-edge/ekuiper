// Copyright 2023 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compressor

import (
	"fmt"
	"io"

	"github.com/lf-edge/ekuiper/pkg/message"
)

type CompressorInstantiator func(name string) (message.Compressor, error)

var compressors = map[string]CompressorInstantiator{}

func GetCompressor(name string) (message.Compressor, error) {
	if instantiator, ok := compressors[name]; ok {
		return instantiator(name)
	}
	return nil, fmt.Errorf("unsupported compressor: %s", name)
}

type CompressWriterIns func(reader io.Writer) (io.Writer, error)

var compressWriters = map[string]CompressWriterIns{}

func GetCompressWriter(name string, writer io.Writer) (io.Writer, error) {
	if instantiator, ok := compressWriters[name]; ok {
		return instantiator(writer)
	}
	return nil, fmt.Errorf("unsupported compressor for file: %s", name)
}
