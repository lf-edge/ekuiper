// Copyright 2023-2024 EMQ Technologies Co., Ltd.
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

package props

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/bwmarrin/snowflake"

	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

var (
	SC     = &StaticConf{props: map[string]string{}}
	sfnode *snowflake.Node
)

type StaticConf struct {
	sync.RWMutex
	props map[string]string
}

func InitProps() {
	envVars := os.Environ()
	for _, envVar := range envVars {
		pair := strings.SplitN(envVar, "=", 2)
		key := pair[0]
		value := pair[1]
		if strings.HasPrefix(key, "KUIPER_PROPS_") {
			shortKey := strings.ToLower(strings.TrimPrefix(key, "KUIPER_PROPS_"))
			SC.Set(shortKey, value)
		}
	}
}

func (s *StaticConf) Get(propName string) (string, bool) {
	switch propName {
	case "et":
		return strconv.FormatInt(timex.GetNowInMilli(), 10), true
	case "snowflake":
		if sfnode == nil {
			var err error
			sfnode, err = snowflake.NewNode(1)
			if err != nil {
				fmt.Printf("fail to create new snowflake node: %v\n", err)
				return "", false
			}
		}
		return sfnode.Generate().String(), true
	default:
		s.RLock()
		defer s.RUnlock()
		v, ok := s.props[propName]
		return v, ok
	}
}

func (s *StaticConf) Set(propName string, value string) {
	s.Lock()
	defer s.Unlock()
	s.props[propName] = value
}
