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
	"github.com/lf-edge/ekuiper/pkg/message"
)

func GetDecompressor(name string) (message.Decompressor, error) {
	switch name {
	case ZLIB:
		return newZlibDecompressor()
	case GZIP:
		return newGzipDecompressor()
	case FLATE:
		return newFlateDecompressor()
	case ZSTD:
		return newzstdDecompressor()
	default:
		return nil, fmt.Errorf("unsupported decompressor: %s", name)
	}
}
