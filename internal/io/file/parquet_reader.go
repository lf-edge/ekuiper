// Copyright 2021-2023 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package file

import (
	"errors"
	"io"
	"os"

	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/parquet-go/parquet-go"
)

type ParquetReader struct {
	f          *os.File
	pf         *parquet.File
	groups     []parquet.RowGroup
	curGroup   int
	rowsReader parquet.Rows
}

func CreateParquetReader(ctx api.StreamContext, filename string, config *FileSourceConfig) (FormatReader, error) {
	var (
		pr  ParquetReader
		err error
	)

	pr.f, err = os.Open(filename)
	if err != nil {
		ctx.GetLogger().Error(err)
		return nil, err
	}

	info, err := pr.f.Stat()
	if err != nil {
		return nil, err
	}

	pr.pf, err = parquet.OpenFile(pr.f, info.Size())
	if err != nil {
		return nil, err
	}

	pr.groups = pr.pf.RowGroups()

	return &pr, nil
}

func (pr *ParquetReader) Read() (map[string]any, error) {
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

func (pr *ParquetReader) Close() error {
	return pr.f.Close()
}
