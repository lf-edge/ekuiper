// Copyright 2023-2023 EMQ Technologies Co., Ltd.
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

package flate

import (
	"bytes"
	"fmt"
	"io"

	"github.com/klauspost/compress/flate"

	"github.com/lf-edge/ekuiper/internal/conf"
)

func NewFlateCompressor() (*flateCompressor, error) {
	flateWriter, err := flate.NewWriter(nil, flate.DefaultCompression)
	if err != nil {
		return nil, err
	}
	return &flateCompressor{
		writer: flateWriter,
	}, nil
}

type flateCompressor struct {
	writer *flate.Writer
	buffer bytes.Buffer
}

func (g *flateCompressor) Compress(data []byte) ([]byte, error) {
	g.buffer.Reset()
	g.writer.Reset(&g.buffer)
	_, err := g.writer.Write(data)
	if err != nil {
		return nil, err
	}
	err = g.writer.Close()
	if err != nil {
		return nil, err
	}
	return g.buffer.Bytes(), nil
}

func NewFlateDecompressor() (*flateDecompressor, error) {
	return &flateDecompressor{reader: flate.NewReader(bytes.NewReader(nil))}, nil
}

type flateDecompressor struct {
	reader io.ReadCloser
}

func (z *flateDecompressor) Decompress(data []byte) ([]byte, error) {
	err := z.reader.(flate.Resetter).Reset(bytes.NewReader(data), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress: %v", err)
	}

	defer func() {
		err := z.reader.Close()
		if err != nil {
			conf.Log.Warnf("failed to close flate decompressor: %v", err)
		}
	}()
	return io.ReadAll(z.reader)
}
