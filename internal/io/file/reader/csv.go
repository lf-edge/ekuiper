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
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/modules"
)

func init() {
	modules.RegisterFileStreamReader("csv", func(ctx api.StreamContext) modules.FileStreamReader {
		return &CsvReader{}
	})
}

type csvConf struct {
	HasHeader bool     `json:"hasHeader"`
	Columns   []string `json:"columns"`
	Delimiter string   `json:"delimiter"`
}

type CsvReader struct {
	csvR   *csv.Reader
	config *csvConf

	cols []string
}

func (r *CsvReader) Provision(ctx api.StreamContext, props map[string]any) error {
	c := &csvConf{
		Delimiter: ",",
	}
	e := cast.MapToStruct(props, c)
	if e != nil {
		return e
	}
	if c.Delimiter == "" {
		c.Delimiter = ","
	}
	r.config = c
	return nil
}

func (r *CsvReader) Bind(ctx api.StreamContext, fileStream io.Reader, _ int) error {
	cr := csv.NewReader(fileStream)
	cr.Comma = rune(r.config.Delimiter[0])
	cr.TrimLeadingSpace = true
	cr.FieldsPerRecord = -1
	cols := r.config.Columns
	if r.config.HasHeader {
		var err error
		ctx.GetLogger().Debug("Has header")
		cols, err = cr.Read()
		if err == io.EOF {
			return fmt.Errorf("header not found")
		}
		if err != nil {
			ctx.GetLogger().Warnf("Read file %s encounter error: %v", "fs.file", err)
			return err
		}
		ctx.GetLogger().Debugf("Got header %v", cols)
	}

	r.csvR = cr
	r.cols = cols
	return nil
}

func (r *CsvReader) Read(ctx api.StreamContext) (any, error) {
	record, err := r.csvR.Read()
	if err == io.EOF {
		return nil, err
	}
	if err != nil {
		ctx.GetLogger().Warnf("Read file %s encounter error: %v", "fs.file", err)
		return nil, err
	}
	ctx.GetLogger().Debugf("Read" + strings.Join(record, ","))

	var m map[string]interface{}
	if r.cols == nil {
		m = make(map[string]interface{}, len(record))
		for i, v := range record {
			m["cols"+strconv.Itoa(i)] = v
		}
	} else {
		m = make(map[string]interface{}, len(r.cols))
		for i, v := range r.cols {
			m[v] = record[i]
		}
	}

	return m, nil
}

func (r *CsvReader) IsBytesReader() bool {
	return false
}

func (r *CsvReader) Close(ctx api.StreamContext) error {
	return nil
}
