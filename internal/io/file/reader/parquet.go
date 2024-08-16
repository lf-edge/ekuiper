// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

//go:build parquet || full

package reader

import (
	"errors"
	"io"
	"os"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/parquet-go/parquet-go"

	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func init() {
	modules.RegisterFileStreamReader("parquet", func(ctx api.StreamContext) modules.FileStreamReader {
		return &ParquetReader{}
	})
}

type ParquetReader struct {
	pf         *parquet.File
	groups     []parquet.RowGroup
	curGroup   int
	rowsReader parquet.Rows
}

func (pr *ParquetReader) Provision(ctx api.StreamContext, props map[string]any) error {
	return nil
}

func (pr *ParquetReader) Bind(ctx api.StreamContext, fr io.Reader, _ int) (err error) {
	f, ok := fr.(*os.File)
	if !ok {
		return errors.New("parquet reader needs a file")
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}

	pr.pf, err = parquet.OpenFile(f, info.Size())
	if err != nil {
		return err
	}
	pr.groups = pr.pf.RowGroups()
	return nil
}

func (pr *ParquetReader) Read(_ api.StreamContext) (any, error) {
	var row [1]parquet.Row
	for pr.curGroup < len(pr.groups) {
		group := pr.groups[pr.curGroup]
		if pr.rowsReader == nil {
			pr.rowsReader = group.Rows()
		}

		_, err := pr.rowsReader.ReadRows(row[:])
		switch {
		case errors.Is(err, io.EOF):
			pr.curGroup++
			err = pr.rowsReader.Close()
			pr.rowsReader = nil
			if err != nil {
				return nil, err
			}
			continue
		case err != nil:
			return nil, err
		}

		m := make(map[string]any)
		err = pr.pf.Schema().Reconstruct(&m, row[0])
		if err != nil {
			return nil, err
		}
		return m, nil
	}
	return nil, io.EOF
}

func (pr *ParquetReader) IsBytesReader() bool {
	return false
}

func (pr *ParquetReader) Close(_ api.StreamContext) error {
	return nil
}

var _ modules.FileStreamReader = &ParquetReader{}
