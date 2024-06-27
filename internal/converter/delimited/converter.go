// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/message"
)

type Converter struct {
	Delimiter string   `json:"delimiter"`
	Cols      []string `json:"cols"`
}

func NewConverter(props map[string]any) (message.Converter, error) {
	c := &Converter{}
	err := cast.MapToStruct(props, c)
	if err != nil {
		return nil, err
	}
	if c.Delimiter == "" {
		c.Delimiter = ","
	}
	return c, nil
}

func (c *Converter) SetColumns(cols []string) {
	c.Cols = cols
}

// Encode If no columns defined, the default order is sort by key
func (c *Converter) Encode(ctx api.StreamContext, d any) (b []byte, err error) {
	defer func() {
		if err != nil {
			err = errorx.NewWithCode(errorx.CovnerterErr, err.Error())
		}
	}()
	switch m := d.(type) {
	case map[string]interface{}:
		sb := &bytes.Buffer{}
		if len(c.Cols) == 0 {
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			c.Cols = keys
		}

		for i, v := range c.Cols {
			if i > 0 {
				sb.WriteString(c.Delimiter)
			}
			fmt.Fprintf(sb, "%v", m[v])
		}
		return sb.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupported type %v, must be a map", d)
	}
}

// Decode If the cols is not set, the default key name is col1, col2, col3...
// The return value is always a map
func (c *Converter) Decode(ctx api.StreamContext, b []byte) (ma any, err error) {
	tokens := strings.Split(string(b), c.Delimiter)
	m := make(map[string]interface{})
	if len(c.Cols) == 0 {
		for i, v := range tokens {
			m["col"+strconv.Itoa(i)] = v
		}
	} else {
		for i, v := range tokens {
			if i < len(c.Cols) {
				m[c.Cols[i]] = v
			}
		}
	}
	return m, nil
}
