// Copyright 2025 EMQ Technologies Co., Ltd.
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

package parquet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/xitongsys/parquet-go/writer"

	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

// ParquetWriter encodes a container in memory. Each Flush returns one complete
// Parquet file; complete files must not be concatenated to append more rows.
type ParquetWriter struct {
	pw  *writer.JSONWriter
	buf bytes.Buffer
}

func (p *ParquetWriter) Encode(ctx api.StreamContext, d any) ([]byte, error) {
	// Keep one-shot encoding independent of any active batch writer.
	batch := &ParquetWriter{}
	if err := batch.Write(ctx, d); err != nil {
		return nil, err
	}
	return batch.Flush(ctx)
}

func (p *ParquetWriter) Decode(_ api.StreamContext, _ []byte) (any, error) {
	return nil, fmt.Errorf("parquet format only supports encoding")
}

func (p *ParquetWriter) New(_ api.StreamContext) error {
	p.pw = nil
	// A previously returned payload may still be in use by a downstream sink.
	p.buf = bytes.Buffer{}
	return nil
}

func (p *ParquetWriter) Write(ctx api.StreamContext, d any) error {
	switch data := d.(type) {
	case map[string]any:
		return p.writeMap(ctx, data)
	case []map[string]any:
		for _, row := range data {
			if err := p.writeMap(ctx, row); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("parquet format cannot encode %T", d)
	}
}

func (p *ParquetWriter) writeMap(ctx api.StreamContext, data map[string]any) error {
	if len(data) == 0 {
		return nil
	}
	if p.pw == nil {
		metadata, err := buildParquetSchema(ctx, data)
		if err != nil {
			return err
		}
		pw, err := writer.NewJSONWriterFromWriter(metadata, &p.buf, 1)
		if err != nil {
			return err
		}
		p.pw = pw
	}
	v, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return p.pw.Write(string(v))
}

func (p *ParquetWriter) Flush(ctx api.StreamContext) ([]byte, error) {
	if p.pw == nil {
		return nil, nil
	}
	if err := p.pw.WriteStop(); err != nil {
		return nil, err
	}
	result := p.buf.Bytes()
	_ = p.New(ctx)
	return result, nil
}

func buildParquetSchema(_ api.StreamContext, data map[string]interface{}) (string, error) {
	schemaBuilder := bytes.NewBufferString(`{"Tag":"name=record","Fields":[`)
	for _, key := range sortedKeys(data) {
		val := data[key]
		subSchema, err := readValue(key, val)
		if err != nil {
			return "", err
		}
		schemaBuilder.WriteString(subSchema)
	}
	return strings.TrimSuffix(schemaBuilder.String(), ",") + `]}`, nil
}

func readValue(name string, val interface{}) (string, error) {
	if name == "" || strings.ContainsAny(name, ",=\\\"") || strings.ContainsFunc(name, unicode.IsControl) {
		return "", fmt.Errorf("unsupported parquet field name %q", name)
	}
	switch d := val.(type) {
	case string:
		return fmt.Sprintf(`{"Tag":"name=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"},`, name), nil
	case int64:
		return fmt.Sprintf(`{"Tag":"name=%s, type=INT64,repetitiontype=OPTIONAL"},`, name), nil
	case float64:
		return fmt.Sprintf(`{"Tag":"name=%s, type=DOUBLE,repetitiontype=OPTIONAL"},`, name), nil
	case bool:
		return fmt.Sprintf(`{"Tag":"name=%s, type=BOOLEAN,repetitiontype=OPTIONAL"},`, name), nil
	case []interface{}:
		return readFromSlice(name, d)
	case map[string]interface{}:
		return readFromMap(name, d)
	}
	return "", fmt.Errorf("unknown type: %T", val)
}

func readFromMap(name string, val map[string]interface{}) (string, error) {
	mapData := fmt.Sprintf(`{"Tag":"name=%s,repetitiontype=OPTIONAL","Fields":[`, name)
	for _, k := range sortedKeys(val) {
		v := val[k]
		data, err := readValue(k, v)
		if err != nil {
			return "", err
		}
		mapData += data
	}
	mapData = strings.TrimSuffix(mapData, ",")
	mapData += `]},`
	return mapData, nil
}

func readFromSlice(name string, val []interface{}) (string, error) {
	mapData := fmt.Sprintf(`{"Tag":"name=%s, type=LIST,repetitiontype=OPTIONAL","Fields":[`, name)
	if len(val) > 0 {
		data, err := readValue("element", val[0])
		if err != nil {
			return "", err
		}
		mapData += data
	} else {
		mapData += fmt.Sprintf(`{"Tag":"name=%s, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"},`, "element")
	}
	mapData = strings.TrimSuffix(mapData, ",")
	mapData += `]},`
	return mapData, nil
}

func sortedKeys(data map[string]any) []string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

var (
	_ message.ConvertWriter = &ParquetWriter{}
	_ message.Converter     = &ParquetWriter{}
)
