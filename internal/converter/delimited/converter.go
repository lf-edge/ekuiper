// Copyright 2022 EMQ Technologies Co., Ltd.
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
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/lf-edge/ekuiper/pkg/message"
)

type Converter struct {
	delimiter string
	cols      []string
}

func NewConverter(delimiter string) (message.Converter, error) {
	if delimiter == "" {
		delimiter = ","
	}
	return &Converter{delimiter: delimiter}, nil
}

func (c *Converter) SetColumns(cols []string) {
	c.cols = cols
}

// Encode If no columns defined, the default order is sort by key
func (c *Converter) Encode(d interface{}) ([]byte, error) {
	switch m := d.(type) {
	case map[string]interface{}:
		var sb strings.Builder
		if len(c.cols) == 0 {
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			c.cols = keys
		}

		for i, v := range c.cols {
			if i > 0 {
				sb.WriteString(c.delimiter)
			}
			sb.WriteString(fmt.Sprintf("%v", m[v]))
		}
		return []byte(sb.String()), nil
	default:
		return nil, fmt.Errorf("unsupported type %v, must be a map", d)
	}
}

// Decode If the cols is not set, the default key name is col1, col2, col3...
// The return value is always a map
func (c *Converter) Decode(b []byte) (interface{}, error) {
	tokens := strings.Split(string(b), c.delimiter)
	m := make(map[string]interface{})
	if len(c.cols) == 0 {
		for i, v := range tokens {
			m["col"+strconv.Itoa(i)] = v
		}
	} else {
		for i, v := range tokens {
			if i < len(c.cols) {
				m[c.cols[i]] = v
			}
		}
	}
	return m, nil
}
