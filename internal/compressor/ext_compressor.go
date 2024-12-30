// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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

//go:build compression || !core

package compressor

import (
	"github.com/lf-edge/ekuiper/v2/internal/compressor/flate"
	"github.com/lf-edge/ekuiper/v2/internal/compressor/gzip"
	"github.com/lf-edge/ekuiper/v2/internal/compressor/zlib"
	"github.com/lf-edge/ekuiper/v2/internal/compressor/zstd"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

const (
	ZLIB  = "zlib"
	GZIP  = "gzip"
	FLATE = "flate"
	ZSTD  = "zstd"
)

func init() {
	compressors[ZLIB] = func(name string, _ map[string]any) (message.Compressor, error) {
		return zlib.NewZlibCompressor()
	}
	compressors[GZIP] = func(name string, _ map[string]any) (message.Compressor, error) {
		return gzip.NewGzipCompressor()
	}
	compressors[FLATE] = func(name string, _ map[string]any) (message.Compressor, error) {
		return flate.NewFlateCompressor()
	}
	compressors[ZSTD] = func(name string, props map[string]any) (message.Compressor, error) {
		return zstd.NewZstdCompressor(props)
	}

	compressWriters[GZIP] = gzip.NewWriter
	compressWriters[ZSTD] = zstd.NewWriter
}
