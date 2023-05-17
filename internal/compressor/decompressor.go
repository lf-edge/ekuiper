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

type DecompressorInstantiator func(name string) (message.Decompressor, error)

var decompressors = map[string]DecompressorInstantiator{}

func GetDecompressor(name string) (message.Decompressor, error) {
	if instantiator, ok := decompressors[name]; ok {
		return instantiator(name)
	}
	return nil, fmt.Errorf("unsupported decompressor: %s", name)
}

type DecompressReaderIns func(reader io.Reader) (io.ReadCloser, error)

var decompressReaders = map[string]DecompressReaderIns{}

func GetDecompressReader(name string, reader io.Reader) (io.ReadCloser, error) {
	if instantiator, ok := decompressReaders[name]; ok {
		return instantiator(reader)
	}
	return nil, fmt.Errorf("unsupported decompressor for file: %s", name)
}
