// Copyright 2026 EMQ Technologies Co., Ltd.
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

package json

import (
	"bytes"
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/valyala/fastjson"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

// LinesConverter converts JSON Lines (newline-delimited JSON) payloads.
// Each JSON object in a payload is one record. When decoding, the objects may
// be separated by any JSON whitespace, including none, so concatenated JSON
// streams are accepted too. When encoding, records are separated by a newline.
type LinesConverter struct {
	converter *FastJsonConverter
	buffer    bytes.Buffer
}

func NewLinesConverter(schema map[string]*ast.JsonStreamField, props map[string]any) *LinesConverter {
	return &LinesConverter{
		converter: NewFastJsonConverter(schema, props),
	}
}

func (c *LinesConverter) ResetSchema(schema map[string]*ast.JsonStreamField) {
	c.converter.ResetSchema(schema)
}

// Decode always returns a list of records, even if the payload has only one
// record. An empty payload decodes to an empty list.
func (c *LinesConverter) Decode(_ api.StreamContext, b []byte) (m any, err error) {
	defer func() {
		if err != nil {
			err = errorx.NewWithCode(errorx.CovnerterErr, err.Error())
		}
	}()
	f := c.converter
	f.RLock()
	defer f.RUnlock()
	var (
		sc     fastjson.Scanner
		maps   []map[string]any
		slices []model.SliceVal
	)
	sc.InitBytes(b)
	n := 1
	for ; sc.Next(); n++ {
		v := sc.Value()
		if v.Type() != fastjson.TypeObject {
			return nil, fmt.Errorf("JSON Lines record %d must be an object but got %s", n, v.Type())
		}
		obj, _ := v.Object()
		if f.isSlice {
			r, err := f.decodeObject2Slice(obj, f.schema, true)
			if err != nil {
				return nil, fmt.Errorf("JSON Lines record %d: %w", n, err)
			}
			slices = append(slices, r)
		} else {
			r, err := f.decodeObject(obj, f.schema, true)
			if err != nil {
				return nil, fmt.Errorf("JSON Lines record %d: %w", n, err)
			}
			maps = append(maps, r)
		}
	}
	if err := sc.Error(); err != nil {
		return nil, fmt.Errorf("JSON Lines record %d: %w", n, err)
	}
	if f.isSlice {
		return slices, nil
	}
	return maps, nil
}

// Encode writes a single record as one JSON line and a list of records as one
// line per record. Every record must encode to a JSON object so that the output
// can be decoded again: a non-nil map, or a slice value when the schema has
// indexes.
func (c *LinesConverter) Encode(ctx api.StreamContext, d any) ([]byte, error) {
	var buf bytes.Buffer
	switch dt := d.(type) {
	case []map[string]any:
		for i, r := range dt {
			if err := c.writeRecord(ctx, &buf, i+1, r); err != nil {
				return nil, err
			}
		}
	case []model.SliceVal:
		for i, r := range dt {
			if err := c.writeRecord(ctx, &buf, i+1, r); err != nil {
				return nil, err
			}
		}
	case []any:
		for i, r := range dt {
			if err := c.writeRecord(ctx, &buf, i+1, r); err != nil {
				return nil, err
			}
		}
	default:
		if err := c.writeRecord(ctx, &buf, 1, d); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func (c *LinesConverter) writeRecord(ctx api.StreamContext, buf *bytes.Buffer, n int, d any) error {
	switch dt := d.(type) {
	case map[string]any:
		if dt == nil {
			return fmt.Errorf("JSON Lines record %d must be an object but got nil map", n)
		}
	case model.SliceVal:
		if !c.converter.isSlice {
			return fmt.Errorf("JSON Lines record %d is a slice value but the schema has no index", n)
		}
	default:
		return fmt.Errorf("JSON Lines record %d must be an object but got %T", n, d)
	}
	b, err := c.converter.Encode(ctx, d)
	if err != nil {
		return err
	}
	writeLine(buf, b)
	return nil
}

func (c *LinesConverter) New(_ api.StreamContext) error {
	c.buffer.Reset()
	return nil
}

func (c *LinesConverter) Write(ctx api.StreamContext, d any) error {
	b, err := c.Encode(ctx, d)
	if err != nil {
		return err
	}
	writeLine(&c.buffer, b)
	return nil
}

// WriteRaw appends an already encoded record. The record may end with line
// terminators (LF or CRLF); they are dropped so that records in a batch are
// always separated by exactly one newline and the batch has no trailing newline.
func (c *LinesConverter) WriteRaw(_ api.StreamContext, raw []byte) error {
	writeLine(&c.buffer, bytes.TrimRight(raw, "\r\n"))
	return nil
}

func (c *LinesConverter) Flush(_ api.StreamContext) ([]byte, error) {
	result := c.buffer.Bytes()
	// Transfer ownership of the completed buffer to the caller so that New and
	// Write cannot mutate the returned data.
	c.buffer = bytes.Buffer{}
	return result, nil
}

// writeLine appends a line to buf, separated from the previous content by a
// newline. Empty lines are skipped so that empty batches add no blank lines.
func writeLine(buf *bytes.Buffer, line []byte) {
	if len(line) == 0 {
		return
	}
	if buf.Len() > 0 {
		buf.WriteByte('\n')
	}
	buf.Write(line)
}

var (
	_ message.Converter                = &LinesConverter{}
	_ message.RawConvertWriter         = &LinesConverter{}
	_ message.SchemaResetAbleConverter = &LinesConverter{}
)
