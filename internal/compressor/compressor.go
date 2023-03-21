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
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/message"
)

func GetCompressor(name string) (message.Compressor, error) {
	switch name {
	case "zlib":
		return &zlibCompressor{}, nil
	default:
		return nil, fmt.Errorf("unsupported compressor: %s", name)
	}
}

type zlibCompressor struct {
	writer *zlib.Writer
}

func (z *zlibCompressor) Close(ctx api.StreamContext) error {
	if z.writer != nil {
		ctx.GetLogger().Infof("closing zlib compressor")
		return z.writer.Close()
	}
	return nil
}

func (z *zlibCompressor) Compress(data []byte) ([]byte, error) {
	var b bytes.Buffer
	if z.writer == nil {
		z.writer = zlib.NewWriter(&b)
	} else {
		z.writer.Reset(&b)
	}
	_, err := z.writer.Write(data)
	if err != nil {
		return nil, err
	}
	err = z.writer.Flush()
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
