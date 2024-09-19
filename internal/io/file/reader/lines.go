// Copyright 2024 EMQ Technologies Co., Ltd.
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

package reader

import (
	"bufio"
	"io"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func init() {
	modules.RegisterFileStreamReader("lines", func(ctx api.StreamContext) modules.FileStreamReader {
		return &LinesReader{}
	})
}

type LinesReader struct {
	scanner *bufio.Scanner
}

func (r *LinesReader) Provision(ctx api.StreamContext, props map[string]any) error {
	return nil
}

func (r *LinesReader) Bind(ctx api.StreamContext, fileStream io.Reader, maxSize int) error {
	if maxSize <= 0 {
		ctx.GetLogger().Errorf("maxSize must be > 0, defaul to 1MB")
		// default to 1MB
		maxSize = 1 << 20
	}
	scanner := bufio.NewScanner(fileStream)
	scanner.Buffer(nil, maxSize)
	scanner.Split(bufio.ScanLines)
	r.scanner = scanner
	return nil
}

func (r *LinesReader) Read(ctx api.StreamContext) (any, error) {
	succ := r.scanner.Scan()
	if !succ {
		return nil, io.EOF
	}
	b := r.scanner.Bytes()
	d := make([]byte, len(b))
	copy(d, b)
	return d, nil
}

func (r *LinesReader) IsBytesReader() bool {
	return true
}

func (r *LinesReader) Close(ctx api.StreamContext) error {
	return nil
}
