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

package cast

import (
	"encoding/base64"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"reflect"
	"strconv"
	"sync"
)

type Strictness int8

const (
	STRICT Strictness = iota
	CONVERT_SAMEKIND
	CONVERT_ALL
)

/*********** Type Cast Utilities *****/

//TODO datetime type
func ToStringAlways(input interface{}) string {
	if input == nil {
		return ""
	}
	return fmt.Sprintf("%v", input)
}

func ToString(input interface{}, sn Strictness) (string, error) {
	switch s := input.(type) {
	case string:
		return s, nil
	case []byte:
		return string(s), nil
	default:
		if sn == CONVERT_ALL {
			switch s := input.(type) {
			case string:
				return s, nil
			case bool:
				return strconv.FormatBool(s), nil
			case float64:
				return strconv.FormatFloat(s, 'f', -1, 64), nil
			case float32:
				return strconv.FormatFloat(float64(s), 'f', -1, 32), nil
			case int:
				return strconv.Itoa(s), nil
			case int64:
				return strconv.FormatInt(s, 10), nil
			case int32:
				return strconv.Itoa(int(s)), nil
			case int16:
				return strconv.FormatInt(int64(s), 10), nil
			case int8:
				return strconv.FormatInt(int64(s), 10), nil
			case uint:
				return strconv.FormatUint(uint64(s), 10), nil
			case uint64:
				return strconv.FormatUint(s, 10), nil
			case uint32:
				return strconv.FormatUint(uint64(s), 10), nil
			case uint16:
				return strconv.FormatUint(uint64(s), 10), nil
			case uint8:
				return strconv.FormatUint(uint64(s), 10), nil
			case nil:
				return "", nil
			case fmt.Stringer:
				return s.String(), nil
			case error:
				return s.Error(), nil
			}
		}
	}
	return "", fmt.Errorf("cannot convert %[1]T(%[1]v) to string", input)
}

func ToInt(input interface{}, sn Strictness) (int, error) {
	switch s := input.(type) {
	case int:
		return s, nil
	case int64:
		return int(s), nil
	case int32:
		return int(s), nil
	case int16:
		return int(s), nil
	case int8:
		return int(s), nil
	case uint:
		return int(s), nil
	case uint64:
		return int(s), nil
	case uint32:
		return int(s), nil
	case uint16:
		return int(s), nil
	case uint8:
		return int(s), nil
	case float64:
		if sn != STRICT || isIntegral64(s) {
			return int(s), nil
		}
	case float32:
		if sn != STRICT || isIntegral32(s) {
			return int(s), nil
		}
	case string:
		if sn == CONVERT_ALL {
			v, err := strconv.ParseInt(s, 0, 0)
			if err == nil {
				return int(v), nil
			}
		}
	case bool:
		if sn == CONVERT_ALL {
			if s {
				return 1, nil
			}
			return 0, nil
		}
	case nil:
		if sn == CONVERT_ALL {
			return 0, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to int", input)
}

func ToInt8(input interface{}, sn Strictness) (int8, error) {
	switch s := input.(type) {
	case int:
		return int8(s), nil
	case int64:
		return int8(s), nil
	case int32:
		return int8(s), nil
	case int16:
		return int8(s), nil
	case int8:
		return s, nil
	case uint:
		return int8(s), nil
	case uint64:
		return int8(s), nil
	case uint32:
		return int8(s), nil
	case uint16:
		return int8(s), nil
	case uint8:
		return int8(s), nil
	case float64:
		if sn != STRICT || isIntegral64(s) {
			return int8(s), nil
		}
	case float32:
		if sn != STRICT || isIntegral32(s) {
			return int8(s), nil
		}
	case string:
		if sn == CONVERT_ALL {
			v, err := strconv.ParseInt(s, 0, 0)
			if err == nil {
				return int8(v), nil
			}
		}
	case bool:
		if sn == CONVERT_ALL {
			if s {
				return 1, nil
			}
			return 0, nil
		}
	case nil:
		if sn == CONVERT_ALL {
			return 0, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to int", input)
}

func ToInt16(input interface{}, sn Strictness) (int16, error) {
	switch s := input.(type) {
	case int:
		return int16(s), nil
	case int64:
		return int16(s), nil
	case int32:
		return int16(s), nil
	case int16:
		return int16(s), nil
	case int8:
		return int16(s), nil
	case uint:
		return int16(s), nil
	case uint64:
		return int16(s), nil
	case uint32:
		return int16(s), nil
	case uint16:
		return int16(s), nil
	case uint8:
		return int16(s), nil
	case float64:
		if sn != STRICT || isIntegral64(s) {
			return int16(s), nil
		}
	case float32:
		if sn != STRICT || isIntegral32(s) {
			return int16(s), nil
		}
	case string:
		if sn == CONVERT_ALL {
			v, err := strconv.ParseInt(s, 0, 0)
			if err == nil {
				return int16(v), nil
			}
		}
	case bool:
		if sn == CONVERT_ALL {
			if s {
				return 1, nil
			}
			return 0, nil
		}
	case nil:
		if sn == CONVERT_ALL {
			return 0, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to int", input)
}

func ToInt32(input interface{}, sn Strictness) (int32, error) {
	switch s := input.(type) {
	case int:
		return int32(s), nil
	case int64:
		return int32(s), nil
	case int32:
		return s, nil
	case int16:
		return int32(s), nil
	case int8:
		return int32(s), nil
	case uint:
		return int32(s), nil
	case uint64:
		return int32(s), nil
	case uint32:
		return int32(s), nil
	case uint16:
		return int32(s), nil
	case uint8:
		return int32(s), nil
	case float64:
		if sn != STRICT || isIntegral64(s) {
			return int32(s), nil
		}
	case float32:
		if sn != STRICT || isIntegral32(s) {
			return int32(s), nil
		}
	case string:
		if sn == CONVERT_ALL {
			v, err := strconv.ParseInt(s, 0, 0)
			if err == nil {
				return int32(v), nil
			}
		}
	case bool:
		if sn == CONVERT_ALL {
			if s {
				return 1, nil
			}
			return 0, nil
		}
	case nil:
		if sn == CONVERT_ALL {
			return 0, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to int", input)
}

func ToInt64(input interface{}, sn Strictness) (int64, error) {
	switch s := input.(type) {
	case int:
		return int64(s), nil
	case int64:
		return s, nil
	case int32:
		return int64(s), nil
	case int16:
		return int64(s), nil
	case int8:
		return int64(s), nil
	case uint:
		return int64(s), nil
	case uint64:
		return int64(s), nil
	case uint32:
		return int64(s), nil
	case uint16:
		return int64(s), nil
	case uint8:
		return int64(s), nil
	case float64:
		if sn != STRICT || isIntegral64(s) {
			return int64(s), nil
		}
	case float32:
		if sn != STRICT || isIntegral32(s) {
			return int64(s), nil
		}
	case string:
		if sn == CONVERT_ALL {
			v, err := strconv.ParseInt(s, 0, 0)
			if err == nil {
				return int64(v), nil
			}
		}
	case bool:
		if sn == CONVERT_ALL {
			if s {
				return 1, nil
			}
			return 0, nil
		}
	case nil:
		if sn == CONVERT_ALL {
			return 0, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to int64", input)
}

func ToFloat64(input interface{}, sn Strictness) (float64, error) {
	switch s := input.(type) {
	case float64:
		return s, nil
	case float32:
		return float64(s), nil
	case int:
		if sn != STRICT {
			return float64(s), nil
		}
	case int64:
		if sn != STRICT {
			return float64(s), nil
		}
	case int32:
		if sn != STRICT {
			return float64(s), nil
		}
	case int16:
		if sn != STRICT {
			return float64(s), nil
		}
	case int8:
		if sn != STRICT {
			return float64(s), nil
		}
	case uint:
		if sn != STRICT {
			return float64(s), nil
		}
	case uint64:
		if sn != STRICT {
			return float64(s), nil
		}
	case uint32:
		if sn != STRICT {
			return float64(s), nil
		}
	case uint16:
		if sn != STRICT {
			return float64(s), nil
		}
	case uint8:
		if sn != STRICT {
			return float64(s), nil
		}
	case string:
		if sn == CONVERT_ALL {
			v, err := strconv.ParseFloat(s, 64)
			if err == nil {
				return v, nil
			}
		}
	case bool:
		if sn == CONVERT_ALL {
			if s {
				return 1, nil
			}
			return 0, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to float64", input)
}

func ToFloat32(input interface{}, sn Strictness) (float32, error) {
	switch s := input.(type) {
	case float64:
		return float32(s), nil
	case float32:
		return s, nil
	case int:
		if sn != STRICT {
			return float32(s), nil
		}
	case int64:
		if sn != STRICT {
			return float32(s), nil
		}
	case int32:
		if sn != STRICT {
			return float32(s), nil
		}
	case int16:
		if sn != STRICT {
			return float32(s), nil
		}
	case int8:
		if sn != STRICT {
			return float32(s), nil
		}
	case uint:
		if sn != STRICT {
			return float32(s), nil
		}
	case uint64:
		if sn != STRICT {
			return float32(s), nil
		}
	case uint32:
		if sn != STRICT {
			return float32(s), nil
		}
	case uint16:
		if sn != STRICT {
			return float32(s), nil
		}
	case uint8:
		if sn != STRICT {
			return float32(s), nil
		}
	case string:
		if sn == CONVERT_ALL {
			v, err := strconv.ParseFloat(s, 32)
			if err == nil {
				return float32(v), nil
			}
		}
	case bool:
		if sn == CONVERT_ALL {
			if s {
				return 1, nil
			}
			return 0, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to float64", input)
}

func ToUint64(i interface{}, sn Strictness) (uint64, error) {
	switch s := i.(type) {
	case string:
		if sn == CONVERT_ALL {
			v, err := strconv.ParseUint(s, 0, 64)
			if err == nil {
				return v, nil
			}
		}
	case int:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint64(s), nil
	case int64:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint64(s), nil
	case int32:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint64(s), nil
	case int16:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint64(s), nil
	case int8:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint64(s), nil
	case uint:
		return uint64(s), nil
	case uint64:
		return s, nil
	case uint32:
		return uint64(s), nil
	case uint16:
		return uint64(s), nil
	case uint8:
		return uint64(s), nil
	case float32:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		if sn != STRICT || isIntegral32(s) {
			return uint64(s), nil
		}
	case float64:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		if sn != STRICT || isIntegral64(s) {
			return uint64(s), nil
		}
	case bool:
		if sn == CONVERT_ALL {
			if s {
				return 1, nil
			}
			return 0, nil
		}
	case nil:
		if sn == CONVERT_ALL {
			return 0, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint", i)
}

func ToUint8(i interface{}, sn Strictness) (uint8, error) {
	switch s := i.(type) {
	case string:
		if sn == CONVERT_ALL {
			v, err := strconv.ParseUint(s, 0, 64)
			if err == nil {
				return uint8(v), nil
			}
		}
	case int:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint8(s), nil
	case int64:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint8(s), nil
	case int32:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint8(s), nil
	case int16:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint8(s), nil
	case int8:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint8(s), nil
	case uint:
		return uint8(s), nil
	case uint64:
		return uint8(s), nil
	case uint32:
		return uint8(s), nil
	case uint16:
		return uint8(s), nil
	case uint8:
		return s, nil
	case float32:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		if sn != STRICT || isIntegral32(s) {
			return uint8(s), nil
		}
	case float64:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		if sn != STRICT || isIntegral64(s) {
			return uint8(s), nil
		}
	case bool:
		if sn == CONVERT_ALL {
			if s {
				return 1, nil
			}
			return 0, nil
		}
	case nil:
		if sn == CONVERT_ALL {
			return 0, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint", i)
}

func ToUint16(i interface{}, sn Strictness) (uint16, error) {
	switch s := i.(type) {
	case string:
		if sn == CONVERT_ALL {
			v, err := strconv.ParseUint(s, 0, 64)
			if err == nil {
				return uint16(v), nil
			}
		}
	case int:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint16(s), nil
	case int64:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint16(s), nil
	case int32:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint16(s), nil
	case int16:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint16(s), nil
	case int8:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint16(s), nil
	case uint:
		return uint16(s), nil
	case uint64:
		return uint16(s), nil
	case uint32:
		return uint16(s), nil
	case uint16:
		return s, nil
	case uint8:
		return uint16(s), nil
	case float32:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		if sn != STRICT || isIntegral32(s) {
			return uint16(s), nil
		}
	case float64:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		if sn != STRICT || isIntegral64(s) {
			return uint16(s), nil
		}
	case bool:
		if sn == CONVERT_ALL {
			if s {
				return 1, nil
			}
			return 0, nil
		}
	case nil:
		if sn == CONVERT_ALL {
			return 0, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint", i)
}

func ToUint32(i interface{}, sn Strictness) (uint32, error) {
	switch s := i.(type) {
	case string:
		if sn == CONVERT_ALL {
			v, err := strconv.ParseUint(s, 0, 64)
			if err == nil {
				return uint32(v), nil
			}
		}
	case int:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint32(s), nil
	case int64:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint32(s), nil
	case int32:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint32(s), nil
	case int16:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint32(s), nil
	case int8:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		return uint32(s), nil
	case uint:
		return uint32(s), nil
	case uint64:
		return uint32(s), nil
	case uint32:
		return s, nil
	case uint16:
		return uint32(s), nil
	case uint8:
		return uint32(s), nil
	case float32:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		if sn != STRICT || isIntegral32(s) {
			return uint32(s), nil
		}
	case float64:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		if sn != STRICT || isIntegral64(s) {
			return uint32(s), nil
		}
	case bool:
		if sn == CONVERT_ALL {
			if s {
				return 1, nil
			}
			return 0, nil
		}
	case nil:
		if sn == CONVERT_ALL {
			return 0, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint", i)
}

func ToBool(input interface{}, sn Strictness) (bool, error) {
	switch b := input.(type) {
	case bool:
		return b, nil
	case nil:
		if sn == CONVERT_ALL {
			return false, nil
		}
	case int:
		if sn == CONVERT_ALL {
			if b != 0 {
				return true, nil
			}
			return false, nil
		}
	case string:
		if sn == CONVERT_ALL {
			return strconv.ParseBool(b)
		}
	}
	return false, fmt.Errorf("cannot convert %[1]T(%[1]v) to bool", input)
}

func ToBytes(input interface{}, sn Strictness) ([]byte, error) {
	switch b := input.(type) {
	case []byte:
		return b, nil
	case string:
		if sn != STRICT {
			return []byte(b), nil
		}
	}
	return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to bytes", input)
}

// ToByteA converts to eKuiper internal byte array
func ToByteA(input interface{}, _ Strictness) ([]byte, error) {
	switch b := input.(type) {
	case []byte:
		return b, nil
	case string:
		r, err := base64.StdEncoding.DecodeString(b)
		if err != nil {
			return nil, fmt.Errorf("illegal string %s, must be base64 encoded string", b)
		}
		return r, nil
	}
	return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to bytes", input)
}

func ToStringMap(input interface{}) (map[string]interface{}, error) {
	var m = map[string]interface{}{}

	switch v := input.(type) {
	case map[interface{}]interface{}:
		for k, val := range v {
			m[ToStringAlways(k)] = val
		}
		return m, nil
	case map[string]interface{}:
		return v, nil
	//case string:
	//	err := jsonStringToObject(v, &m)
	//	return m, err
	default:
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to map", input)
	}
}

func ToTypedSlice(input interface{}, conv func(interface{}, Strictness) (interface{}, error), eleType string, sn Strictness) (interface{}, error) {
	s := reflect.ValueOf(input)
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to %s slice)", input, eleType)
	}
	if s.Len() == 0 {
		result := reflect.MakeSlice(reflect.TypeOf([]interface{}{}), s.Len(), s.Len())
		return result.Interface(), nil
	}
	ele, err := conv(s.Index(0).Interface(), sn)
	et := reflect.TypeOf(ele)
	if err != nil || et == nil {
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to %s slice for the %d element: %v", input, eleType, 0, err)
	}
	result := reflect.MakeSlice(reflect.SliceOf(et), s.Len(), s.Len())
	result.Index(0).Set(reflect.ValueOf(ele))
	for i := 1; i < s.Len(); i++ {
		ele, err := conv(s.Index(i).Interface(), sn)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to int slice for the %d element: %v", input, i, err)
		}
		result.Index(i).Set(reflect.ValueOf(ele))
	}
	return result.Interface(), nil
}

func ToInt64Slice(input interface{}, sn Strictness) ([]int64, error) {
	s := reflect.ValueOf(input)
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to int slice)", input)
	}
	var result []int64
	for i := 0; i < s.Len(); i++ {
		ele, err := ToInt64(s.Index(i).Interface(), sn)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to int slice for the %d element: %v", input, i, err)
		}
		result = append(result, ele)
	}
	return result, nil
}

func ToUint64Slice(input interface{}, sn Strictness) ([]uint64, error) {
	s := reflect.ValueOf(input)
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint slice)", input)
	}
	var result []uint64
	for i := 0; i < s.Len(); i++ {
		ele, err := ToUint64(s.Index(i).Interface(), sn)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint slice for the %d element: %v", input, i, err)
		}
		result = append(result, ele)
	}
	return result, nil
}

func ToFloat64Slice(input interface{}, sn Strictness) ([]float64, error) {
	s := reflect.ValueOf(input)
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to float slice)", input)
	}
	var result []float64
	for i := 0; i < s.Len(); i++ {
		ele, err := ToFloat64(s.Index(i).Interface(), sn)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to float slice for the %d element: %v", input, i, err)
		}
		result = append(result, ele)
	}
	return result, nil
}

func ToFloat32Slice(input interface{}, sn Strictness) ([]float32, error) {
	s := reflect.ValueOf(input)
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to float slice)", input)
	}
	var result []float32
	for i := 0; i < s.Len(); i++ {
		ele, err := ToFloat32(s.Index(i).Interface(), sn)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to float slice for the %d element: %v", input, i, err)
		}
		result = append(result, ele)
	}
	return result, nil
}

func ToBoolSlice(input interface{}, sn Strictness) ([]bool, error) {
	s := reflect.ValueOf(input)
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to bool slice)", input)
	}
	var result []bool
	for i := 0; i < s.Len(); i++ {
		ele, err := ToBool(s.Index(i).Interface(), sn)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to bool slice for the %d element: %v", input, i, err)
		}
		result = append(result, ele)
	}
	return result, nil
}

func ToStringSlice(input interface{}, sn Strictness) ([]string, error) {
	s := reflect.ValueOf(input)
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to string slice)", input)
	}
	var result []string
	for i := 0; i < s.Len(); i++ {
		ele, err := ToString(s.Index(i).Interface(), sn)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to string slice for the %d element: %v", input, i, err)
		}
		result = append(result, ele)
	}
	return result, nil
}

func ToBytesSlice(input interface{}, sn Strictness) ([][]byte, error) {
	s := reflect.ValueOf(input)
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to string slice)", input)
	}
	var result [][]byte
	for i := 0; i < s.Len(); i++ {
		ele, err := ToBytes(s.Index(i).Interface(), sn)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to bytes slice for the %d element: %v", input, i, err)
		}
		result = append(result, ele)
	}
	return result, nil
}

//MapToStruct
/*
*   Convert a map into a struct. The output parameter must be a pointer to a struct
*   The struct can have the json meta data
 */
func MapToStruct(input, output interface{}) error {
	config := &mapstructure.DecoderConfig{
		TagName: "json",
		Result:  output,
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}

	return decoder.Decode(input)
}

// MapToStructStrict
/*
*   Convert a map into a struct. The output parameter must be a pointer to a struct
*   If the input have key/value pair output do not defined, will report error
 */
func MapToStructStrict(input, output interface{}) error {
	config := &mapstructure.DecoderConfig{
		ErrorUnused: true,
		TagName:     "json",
		Result:      output,
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		return err
	}

	return decoder.Decode(input)
}

func ConvertMap(s map[interface{}]interface{}) map[string]interface{} {
	r := make(map[string]interface{})
	for k, v := range s {
		switch t := v.(type) {
		case map[interface{}]interface{}:
			v = ConvertMap(t)
		case []interface{}:
			v = ConvertArray(t)
		}
		r[fmt.Sprintf("%v", k)] = v
	}
	return r
}

func ConvertArray(s []interface{}) []interface{} {
	r := make([]interface{}, len(s))
	for i, e := range s {
		switch t := e.(type) {
		case map[interface{}]interface{}:
			e = ConvertMap(t)
		case []interface{}:
			e = ConvertArray(t)
		}
		r[i] = e
	}
	return r
}

func SyncMapToMap(sm *sync.Map) map[string]interface{} {
	m := make(map[string]interface{})
	sm.Range(func(k interface{}, v interface{}) bool {
		m[fmt.Sprintf("%v", k)] = v
		return true
	})
	return m
}
func MapToSyncMap(m map[string]interface{}) *sync.Map {
	sm := new(sync.Map)
	for k, v := range m {
		sm.Store(k, v)
	}
	return sm
}

func isIntegral64(val float64) bool {
	return val == float64(int(val))
}

func isIntegral32(val float32) bool {
	return val == float32(int(val))
}

func ConvertToInterfaceArr(orig map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range orig {
		vt := reflect.TypeOf(v)
		if vt == nil {
			result[k] = nil
			continue
		}
		switch vt.Kind() {
		case reflect.Slice:
			result[k] = ConvertSlice(v)
		case reflect.Map:
			result[k] = ConvertToInterfaceArr(v.(map[string]interface{}))
		default:
			result[k] = v
		}
	}
	return result
}

func ConvertSlice(v interface{}) []interface{} {
	value := reflect.ValueOf(v)
	tempArr := make([]interface{}, value.Len())
	for i := 0; i < value.Len(); i++ {
		item := value.Index(i)
		if item.Kind() == reflect.Map {
			tempArr[i] = ConvertToInterfaceArr(item.Interface().(map[string]interface{}))
		} else if item.Kind() == reflect.Slice {
			tempArr[i] = ConvertSlice(item.Interface())
		} else {
			tempArr[i] = item.Interface()
		}
	}
	return tempArr
}
