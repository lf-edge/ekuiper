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
	"bytes"
	"compress/zlib"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/message"
	"io"
)

func GetDecompressor(name string) (message.Decompressor, error) {
	switch name {
	case "zlib":
		return &zlibDecompressor{}, nil
	default:
		return nil, fmt.Errorf("unsupported compressor: %s", name)
	}
}

type zlibDecompressor struct {
	reader io.ReadCloser
}

func (z *zlibDecompressor) Decompress(data []byte) ([]byte, error) {
	if z.reader == nil {
		r, err := zlib.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("failed to decompress: %v", err)
		}
		z.reader = r
	} else {
		err := z.reader.(zlib.Resetter).Reset(bytes.NewReader(data), nil)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress: %v", err)
		}
	}
	defer func() {
		err := z.reader.Close()
		if err != nil {
			conf.Log.Warnf("failed to close zlib decompressor: %v", err)
		}
	}()
	return io.ReadAll(z.reader)
}
