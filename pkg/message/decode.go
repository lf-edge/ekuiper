// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package message

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	FormatBinary   = "binary"
	FormatJson     = "json"
	FormatProtobuf = "protobuf"

	DefaultField = "self"
	MetaKey      = "__meta"
)

func Decode(payload []byte, format string) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	switch strings.ToLower(format) {
	case FormatJson:
		e := json.Unmarshal(payload, &result)
		return result, e
	case FormatBinary:
		result[DefaultField] = payload
		return result, nil
	}
	return nil, fmt.Errorf("invalid format %s", format)
}
