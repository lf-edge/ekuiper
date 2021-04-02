package common

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"sync"
)

/*********** Type Cast Utilities *****/
//TODO datetime type
func ToStringAlways(input interface{}) string {
	return fmt.Sprintf("%v", input)
}

func ToString(input interface{}, unstrict bool) (string, error) {
	switch s := input.(type) {
	case string:
		return s, nil
	default:
		if unstrict {
			return ToStringAlways(input), nil
		}
	}
	return "", fmt.Errorf("cannot convert %[1]T(%[1]v) to string", input)
}

func ToInt(input interface{}, unstrict bool) (int, error) {
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
		if unstrict || isIntegral64(s) {
			return int(s), nil
		}
	case float32:
		if unstrict || isIntegral32(s) {
			return int(s), nil
		}
	case string:
		if unstrict {
			v, err := strconv.ParseInt(s, 0, 0)
			if err == nil {
				return int(v), nil
			}
		}
	case bool:
		if unstrict {
			if s {
				return 1, nil
			}
			return 0, nil
		}
	case nil:
		if unstrict {
			return 0, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to int", input)
}

func ToInt64(input interface{}, unstrict bool) (int64, error) {
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
		if unstrict || isIntegral64(s) {
			return int64(s), nil
		}
	case float32:
		if unstrict || isIntegral32(s) {
			return int64(s), nil
		}
	case string:
		if unstrict {
			v, err := strconv.ParseInt(s, 0, 0)
			if err == nil {
				return int64(v), nil
			}
		}
	case bool:
		if unstrict {
			if s {
				return 1, nil
			}
			return 0, nil
		}
	case nil:
		if unstrict {
			return 0, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to int64", input)
}

func ToFloat64(input interface{}, unstrict bool) (float64, error) {
	switch s := input.(type) {
	case float64:
		return s, nil
	case float32:
		return float64(s), nil
	case int:
		if unstrict {
			return float64(s), nil
		}
	case int64:
		if unstrict {
			return float64(s), nil
		}
	case int32:
		if unstrict {
			return float64(s), nil
		}
	case int16:
		if unstrict {
			return float64(s), nil
		}
	case int8:
		if unstrict {
			return float64(s), nil
		}
	case uint:
		if unstrict {
			return float64(s), nil
		}
	case uint64:
		if unstrict {
			return float64(s), nil
		}
	case uint32:
		if unstrict {
			return float64(s), nil
		}
	case uint16:
		if unstrict {
			return float64(s), nil
		}
	case uint8:
		if unstrict {
			return float64(s), nil
		}
	case string:
		if unstrict {
			v, err := strconv.ParseFloat(s, 64)
			if err == nil {
				return v, nil
			}
		}
	case bool:
		if unstrict {
			if s {
				return 1, nil
			}
			return 0, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to float64", input)
}

func ToUint64(i interface{}, unstrict bool) (uint64, error) {
	switch s := i.(type) {
	case string:
		if unstrict {
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
		if unstrict || isIntegral32(s) {
			return uint64(s), nil
		}
	case float64:
		if s < 0 {
			return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint, negative not allowed", i)
		}
		if unstrict || isIntegral64(s) {
			return uint64(s), nil
		}
	case bool:
		if unstrict {
			if s {
				return 1, nil
			}
			return 0, nil
		}
	case nil:
		if unstrict {
			return 0, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint", i)
}

func ToBool(input interface{}, unstrict bool) (bool, error) {
	switch b := input.(type) {
	case bool:
		return b, nil
	case nil:
		if unstrict {
			return false, nil
		}
	case int:
		if unstrict {
			if b != 0 {
				return true, nil
			}
			return false, nil
		}
	case string:
		if unstrict {
			return strconv.ParseBool(b)
		}
	}
	return false, fmt.Errorf("cannot convert %[1]T(%[1]v) to bool", input)
}

func ToBytes(input interface{}, _ bool) ([]byte, error) {
	switch b := input.(type) {
	case []byte:
		return b, nil
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

func ToTypedSlice(input interface{}, conv func(input interface{}, unstrict bool) (interface{}, error), eleType string, unstrict bool) (interface{}, error) {
	s := reflect.ValueOf(input)
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to %s slice)", input, eleType)
	}
	ele, err := conv(s.Index(0).Interface(), unstrict)
	if err != nil {
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to %s slice for the %d element: %v", input, eleType, 0, err)
	}
	result := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(ele)), s.Len(), s.Len())
	result.Index(0).Set(reflect.ValueOf(ele))
	for i := 1; i < s.Len(); i++ {
		ele, err := conv(s.Index(i).Interface(), unstrict)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to int slice for the %d element: %v", input, i, err)
		}
		result.Index(i).Set(reflect.ValueOf(ele))
	}
	return result.Interface(), nil
}

func ToInt64Slice(input interface{}, unstrict bool) ([]int64, error) {
	s := reflect.ValueOf(input)
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to int slice)", input)
	}
	var result []int64
	for i := 0; i < s.Len(); i++ {
		ele, err := ToInt64(s.Index(i).Interface(), unstrict)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to int slice for the %d element: %v", input, i, err)
		}
		result = append(result, ele)
	}
	return result, nil
}

func ToUint64Slice(input interface{}, unstrict bool) ([]uint64, error) {
	s := reflect.ValueOf(input)
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint slice)", input)
	}
	var result []uint64
	for i := 0; i < s.Len(); i++ {
		ele, err := ToUint64(s.Index(i).Interface(), unstrict)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to uint slice for the %d element: %v", input, i, err)
		}
		result = append(result, ele)
	}
	return result, nil
}

func ToFloat64Slice(input interface{}, unstrict bool) ([]float64, error) {
	s := reflect.ValueOf(input)
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to float slice)", input)
	}
	var result []float64
	for i := 0; i < s.Len(); i++ {
		ele, err := ToFloat64(s.Index(i).Interface(), unstrict)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to float slice for the %d element: %v", input, i, err)
		}
		result = append(result, ele)
	}
	return result, nil
}

func ToBoolSlice(input interface{}, unstrict bool) ([]bool, error) {
	s := reflect.ValueOf(input)
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to bool slice)", input)
	}
	var result []bool
	for i := 0; i < s.Len(); i++ {
		ele, err := ToBool(s.Index(i).Interface(), unstrict)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to bool slice for the %d element: %v", input, i, err)
		}
		result = append(result, ele)
	}
	return result, nil
}

func ToStringSlice(input interface{}, unstrict bool) ([]string, error) {
	s := reflect.ValueOf(input)
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to string slice)", input)
	}
	var result []string
	for i := 0; i < s.Len(); i++ {
		ele, err := ToString(s.Index(i).Interface(), unstrict)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to string slice for the %d element: %v", input, i, err)
		}
		result = append(result, ele)
	}
	return result, nil
}

func ToBytesSlice(input interface{}, unstrict bool) ([][]byte, error) {
	s := reflect.ValueOf(input)
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to string slice)", input)
	}
	var result [][]byte
	for i := 0; i < s.Len(); i++ {
		ele, err := ToBytes(s.Index(i).Interface(), unstrict)
		if err != nil {
			return nil, fmt.Errorf("cannot convert %[1]T(%[1]v) to bytes slice for the %d element: %v", input, i, err)
		}
		result = append(result, ele)
	}
	return result, nil
}

/*
*   Convert a map into a struct. The output parameter must be a pointer to a struct
*   The struct can have the json meta data
 */
func MapToStruct(input, output interface{}) error {
	// convert map to json
	jsonString, err := json.Marshal(input)
	if err != nil {
		return err
	}

	// convert json to struct
	return json.Unmarshal(jsonString, output)
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
