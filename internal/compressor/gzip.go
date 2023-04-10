// Copyright 2023 carlclone@gmail.com.
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
	"fmt"
	"github.com/klauspost/compress/gzip"
	"github.com/lf-edge/ekuiper/internal/conf"
	"io"
)

func newGzipCompressor() (*gzipCompressor, error) {
	return &gzipCompressor{
		writer: gzip.NewWriter(nil),
	}, nil
}

type gzipCompressor struct {
	writer *gzip.Writer
	buffer bytes.Buffer
}

func (g *gzipCompressor) Compress(data []byte) ([]byte, error) {
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

func newGzipDecompressor() (*gzipDecompressor, error) {
	return &gzipDecompressor{}, nil
}

type gzipDecompressor struct {
	reader *gzip.Reader
}

func (z *gzipDecompressor) Decompress(data []byte) ([]byte, error) {
	if z.reader == nil {
		r, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("failed to decompress: %v", err)
		}
		z.reader = r
	} else {
		err := z.reader.Reset(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("failed to decompress: %v", err)
		}
	}
	defer func() {
		err := z.reader.Close()
		if err != nil {
			conf.Log.Warnf("failed to close gzip decompressor: %v", err)
		}
	}()
	return io.ReadAll(z.reader)
}
