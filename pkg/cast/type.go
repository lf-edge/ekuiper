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

package cast

import (
	"encoding/json"
	"fmt"
	"time"
)

type DurationConf time.Duration

func (dp *DurationConf) UnmarshalJSON(data []byte) error {
	var duration any
	if err := json.Unmarshal(data, &duration); err != nil {
		return err
	}
	dd, err := ConvertDuration(duration)
	if err != nil {
		return err
	}
	*dp = DurationConf(dd)
	return nil
}

func (d DurationConf) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, time.Duration(d).String())), nil
}

func (dp *DurationConf) UnmarshalYAML(unmarshal func(any) error) error {
	var duration any
	if err := unmarshal(&duration); err != nil {
		return err
	}
	dd, err := ConvertDuration(duration)
	if err != nil {
		return err
	}
	*dp = DurationConf(dd)
	return nil
}

func (d DurationConf) MarshalYAML() (any, error) {
	return time.Duration(d).String(), nil
}

func ConvertDuration(s any) (time.Duration, error) {
	switch x := s.(type) {
	case string:
		return time.ParseDuration(x)
	case int:
		return time.Duration(x) * time.Millisecond, nil
	case float64: // from json
		d, err := ToInt64(x, STRICT)
		if err != nil {
			return 0, fmt.Errorf("duration %v is not an integer", x)
		}
		return time.Duration(d) * time.Millisecond, nil
	}
	return 0, fmt.Errorf("unsupported type:%t", s)
}

type TypedNil struct{}

var TNil = (*TypedNil)(nil)
