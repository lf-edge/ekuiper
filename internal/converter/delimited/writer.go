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

package delimited

import (
	"bytes"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

type CsvWriter struct {
	// The internal writer. When flushing, create a new one.
	converter *Converter
	buffer    *bytes.Buffer
	header    string
	headerSet bool
}

func NewCsvWriter(_ api.StreamContext, props map[string]any) (message.ConvertWriter, error) {
	c, err := NewConverter(props)
	if err != nil {
		return nil, err
	}
	cc := c.(*Converter)
	// Header are now creating by batch writer
	cc.HasHeader = false
	return &CsvWriter{
		converter: cc,
		buffer:    bytes.NewBuffer(nil),
	}, nil
}

func (w *CsvWriter) New(ctx api.StreamContext) error {
	ctx.GetLogger().Debugf("new csv writer")
	w.buffer.Reset()
	w.header = ""
	w.headerSet = false
	return nil
}

func (w *CsvWriter) Write(ctx api.StreamContext, d any) error {
	ctx.GetLogger().Debugf("csv writer write")
	result, err := w.converter.Encode(ctx, d)
	if err != nil {
		return err
	}
	if !w.headerSet {
		if w.buffer.Len() > 0 {
			w.buffer.WriteString("\n")
		}
		w.header = strings.Join(w.converter.Cols, w.converter.Delimiter)
		w.buffer.WriteString(w.header)
		w.headerSet = true
	}
	w.buffer.WriteString("\n")
	w.buffer.Write(result)
	return nil
}

func (w *CsvWriter) WriteRaw(ctx api.StreamContext, raw []byte) error {
	ctx.GetLogger().Debugf("csv writer write raw")
	if w.buffer.Len() > 0 {
		w.buffer.WriteString("\n")
	}
	w.buffer.Write(raw)
	return nil
}

func (w *CsvWriter) Flush(ctx api.StreamContext) ([]byte, error) {
	ctx.GetLogger().Debugf("csv writer flush")
	result := w.buffer.Bytes()
	// Transfer ownership of the completed buffer to the caller.
	w.buffer = bytes.NewBuffer(nil)
	return result, nil
}

var _ message.RawConvertWriter = &CsvWriter{}
