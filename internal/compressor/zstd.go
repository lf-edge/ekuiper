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
	"github.com/klauspost/compress/zstd"
)

func newZstdCompressor() (*zstdCompressor, error) {
	zstdWriter, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	return &zstdCompressor{
		writer: zstdWriter,
	}, nil
}

type zstdCompressor struct {
	writer *zstd.Encoder
	buffer bytes.Buffer
}

func (g *zstdCompressor) Compress(data []byte) ([]byte, error) {
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

func newzstdDecompressor() (*zstdDecompressor, error) {
	r, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	if err != nil {
		return nil, err
	}
	return &zstdDecompressor{decoder: r}, nil
}

type zstdDecompressor struct {
	decoder *zstd.Decoder
}

func (z *zstdDecompressor) Decompress(data []byte) ([]byte, error) {
	return z.decoder.DecodeAll(data, nil)
}
