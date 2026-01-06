// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

type Converter struct {
	Delimiter string   `json:"delimiter"`
	Cols      []string `json:"fields"`
	HasHeader bool     `json:"hasHeader"`
}

func NewConverter(props map[string]any) (message.Converter, error) {
	c := &Converter{}
	err := cast.MapToStruct(props, c)
	if err != nil {
		return nil, err
	}
	// Set default delimiter if not provided
	if c.Delimiter == "" {
		c.Delimiter = ","
	}
	return c, nil
}

// Encode If no columns defined, the default order is sort by key
func (c *Converter) Encode(ctx api.StreamContext, d any) (b []byte, err error) {
	defer func() {
		if err != nil {
			err = errorx.NewWithCode(errorx.CovnerterErr, err.Error())
		}
	}()
	switch m := d.(type) {
	case model.SliceVal:
		sb := &bytes.Buffer{}
		if len(c.Cols) > 0 && c.HasHeader {
			hb := []byte(strings.Join(c.Cols, c.Delimiter))
			sb.WriteString(c.Delimiter)
			_ = binary.Write(sb, binary.BigEndian, uint32(len(hb)))
			sb.Write(hb)
		}
		for i, v := range m {
			if i > 0 {
				sb.WriteString(c.Delimiter)
			}
			p, _ := cast.ToString(v, cast.CONVERT_ALL)
			sb.WriteString(p)
		}
		return sb.Bytes(), nil
	case []model.SliceVal:
		sb := &bytes.Buffer{}
		if len(c.Cols) > 0 && c.HasHeader {
			hb := []byte(strings.Join(c.Cols, c.Delimiter))
			sb.Write(hb)
			sb.WriteString("\n")
		}
		for i, mm := range m {
			if i > 0 {
				sb.WriteString("\n")
			}
			for j, v := range mm {
				if j > 0 {
					sb.WriteString(c.Delimiter)
				}
				p, _ := cast.ToString(v, cast.CONVERT_ALL)
				sb.WriteString(p)
			}
		}
		return sb.Bytes(), nil
	case map[string]any:
		sb := &bytes.Buffer{}
		if len(c.Cols) == 0 {
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			c.Cols = keys
			if len(c.Cols) > 0 && c.HasHeader {
				hb := []byte(strings.Join(c.Cols, c.Delimiter))
				sb.WriteString(c.Delimiter)
				_ = binary.Write(sb, binary.BigEndian, uint32(len(hb)))
				sb.Write(hb)
				ctx.GetLogger().Infof("delimiter header %s", hb)
			}
		}
		for i, v := range c.Cols {
			if i > 0 {
				sb.WriteString(c.Delimiter)
			}
			p, _ := cast.ToString(m[v], cast.CONVERT_ALL)
			sb.WriteString(p)
		}
		return sb.Bytes(), nil
	case []map[string]any:
		sb := &bytes.Buffer{}
		var cols []string
		for i, mm := range m {
			if i > 0 {
				sb.WriteString("\n")
			}
			if len(cols) == 0 {
				keys := make([]string, 0, len(mm))
				for k := range mm {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				cols = keys
				if len(c.Cols) == 0 {
					c.Cols = cols
				}
				if len(cols) > 0 && c.HasHeader {
					hb := []byte(strings.Join(cols, c.Delimiter))
					sb.Write(hb)
					sb.WriteString("\n")
				}
			}
			for j, v := range cols {
				if j > 0 {
					sb.WriteString(c.Delimiter)
				}
				p, _ := cast.ToString(mm[v], cast.CONVERT_ALL)
				sb.WriteString(p)
			}
		}
		return sb.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported type %v, must be a map", d)
	}
}

// Decode If the cols is not set, the default key name is col1, col2, col3...
// If hasHeader is true, the first line is treated as column names.
// Returns a map for single-line input, or []map[string]any for multi-line input.
// Note: Does not handle quoted fields or escape characters (use encoding/csv if needed).
func (c *Converter) Decode(ctx api.StreamContext, b []byte) (ma any, err error) {
	defer func() {
		if err != nil {
			err = errorx.NewWithCode(errorx.CovnerterErr, err.Error())
		}
	}()

	input := strings.TrimSpace(string(b))
	if input == "" {
		return make(map[string]interface{}), nil
	}

	lines := strings.Split(input, "\n")

	// Determine columns to use
	cols := c.Cols
	startIdx := 0

	if c.HasHeader && len(lines) > 0 {
		// First line is the header
		headerTokens := strings.Split(strings.TrimSpace(lines[0]), c.Delimiter)
		cols = make([]string, len(headerTokens))
		for i, token := range headerTokens {
			cols[i] = strings.TrimSpace(token)
		}
		startIdx = 1
	}

	// If no columns determined, use default naming
	if len(cols) == 0 && startIdx < len(lines) {
		// Determine column count from the first data line
		firstLineTokens := strings.Split(strings.TrimSpace(lines[startIdx]), c.Delimiter)
		cols = make([]string, len(firstLineTokens))
		for i := range firstLineTokens {
			cols[i] = "col" + strconv.Itoa(i)
		}
	}

	// Parse data lines
	dataLines := lines[startIdx:]
	if len(dataLines) == 0 {
		return make(map[string]interface{}), nil
	}

	if len(dataLines) == 1 {
		// Single line - return a single map
		tokens := strings.Split(strings.TrimSpace(dataLines[0]), c.Delimiter)
		m := make(map[string]interface{}, len(cols))
		for i, v := range tokens {
			if i < len(cols) {
				m[cols[i]] = strings.TrimSpace(v)
			} else if ctx != nil {
				ctx.GetLogger().Debugf("field count mismatch: expected %d columns, got %d", len(cols), len(tokens))
				break
			}
		}
		return m, nil
	}

	// Multiple lines - return []map[string]any
	// Pre-allocate with exact size
	result := make([]map[string]interface{}, len(dataLines))
	idx := 0
	for _, line := range dataLines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		tokens := strings.Split(line, c.Delimiter)
		// Pre-allocate map with column count
		m := make(map[string]interface{}, len(cols))
		for i, v := range tokens {
			if i < len(cols) {
				m[cols[i]] = strings.TrimSpace(v)
			} else if ctx != nil {
				ctx.GetLogger().Debugf("field count mismatch: expected %d columns, got %d", len(cols), len(tokens))
				break
			}
		}
		result[idx] = m
		idx++
	}
	// Trim slice if empty lines were skipped
	if idx < len(result) {
		result = result[:idx]
	}
	return result, nil
}
