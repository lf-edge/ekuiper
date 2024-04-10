// Copyright 2023 EMQ Technologies Co., Ltd.
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

package function

import (
	"fmt"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

// The functions here are used to implement the array functions to be referred in
// 1. Aggregate function
// 2. Array function

func max(arr []interface{}) (interface{}, bool) {
	if len(arr) > 0 {
		v := getFirstValidArg(arr)
		switch t := v.(type) {
		case int:
			if r, err := sliceIntMax(arr, int64(t)); err != nil {
				return err, false
			} else {
				return r, true
			}
		case int64:
			if r, err := sliceIntMax(arr, t); err != nil {
				return err, false
			} else {
				return r, true
			}
		case float64:
			if r, err := sliceFloatMax(arr, t); err != nil {
				return err, false
			} else {
				return r, true
			}
		case string:
			if r, err := sliceStringMax(arr, t); err != nil {
				return err, false
			} else {
				return r, true
			}
		case nil:
			return nil, true
		default:
			return fmt.Errorf("found invalid arg %[1]T(%[1]v)", v), false
		}
	}
	return nil, true
}

func min(arr []interface{}) (interface{}, bool) {
	if len(arr) > 0 {
		v := getFirstValidArg(arr)
		switch t := v.(type) {
		case int:
			if r, err := sliceIntMin(arr, int64(t)); err != nil {
				return err, false
			} else {
				return r, true
			}
		case int64:
			if r, err := sliceIntMin(arr, t); err != nil {
				return err, false
			} else {
				return r, true
			}
		case float64:
			if r, err := sliceFloatMin(arr, t); err != nil {
				return err, false
			} else {
				return r, true
			}
		case string:
			if r, err := sliceStringMin(arr, t); err != nil {
				return err, false
			} else {
				return r, true
			}
		case nil:
			return nil, true
		default:
			return fmt.Errorf("found invalid arg %[1]T(%[1]v)", v), false
		}
	}
	return nil, true
}

func getCount(s []interface{}) int {
	c := 0
	for _, v := range s {
		if v != nil {
			c++
		}
	}
	return c
}

func getFirstValidArg(s []interface{}) interface{} {
	for _, v := range s {
		if v != nil {
			return v
		}
	}
	return nil
}

func sliceIntTotal(s []interface{}) (int64, error) {
	var total int64
	for _, v := range s {
		if v == nil {
			continue
		}
		vi, err := cast.ToInt64(v, cast.CONVERT_SAMEKIND)
		if err == nil {
			total += vi
		} else if v != nil {
			return 0, fmt.Errorf("requires int but found %[1]T(%[1]v)", v)
		}
	}
	return total, nil
}

func sliceFloatTotal(s []interface{}) (float64, error) {
	var total float64
	for _, v := range s {
		if v == nil {
			continue
		}
		if vf, ok := v.(float64); ok {
			total += vf
		} else if v != nil {
			return 0, fmt.Errorf("requires float64 but found %[1]T(%[1]v)", v)
		}
	}
	return total, nil
}

func sliceIntMax(s []interface{}, max int64) (int64, error) {
	for _, v := range s {
		if v == nil {
			continue
		}
		vi, err := cast.ToInt64(v, cast.CONVERT_SAMEKIND)
		if err == nil {
			if vi > max {
				max = vi
			}
		} else if v != nil {
			return 0, fmt.Errorf("requires int64 but found %[1]T(%[1]v)", v)
		}
	}
	return max, nil
}

func sliceFloatMax(s []interface{}, max float64) (float64, error) {
	for _, v := range s {
		if v == nil {
			continue
		}
		if vf, ok := v.(float64); ok {
			if max < vf {
				max = vf
			}
		} else if v != nil {
			return 0, fmt.Errorf("requires float64 but found %[1]T(%[1]v)", v)
		}
	}
	return max, nil
}

func sliceStringMax(s []interface{}, max string) (string, error) {
	for _, v := range s {
		if v == nil {
			continue
		}
		if vs, ok := v.(string); ok {
			if max < vs {
				max = vs
			}
		} else if v != nil {
			return "", fmt.Errorf("requires string but found %[1]T(%[1]v)", v)
		}
	}
	return max, nil
}

func sliceIntMin(s []interface{}, min int64) (int64, error) {
	for _, v := range s {
		if v == nil {
			continue
		}
		vi, err := cast.ToInt64(v, cast.CONVERT_SAMEKIND)
		if err == nil {
			if vi < min {
				min = vi
			}
		} else if v != nil {
			return 0, fmt.Errorf("requires int64 but found %[1]T(%[1]v)", v)
		}
	}
	return min, nil
}

func sliceFloatMin(s []interface{}, min float64) (float64, error) {
	for _, v := range s {
		if v == nil {
			continue
		}
		if vf, ok := v.(float64); ok {
			if min > vf {
				min = vf
			}
		} else if v != nil {
			return 0, fmt.Errorf("requires float64 but found %[1]T(%[1]v)", v)
		}
	}
	return min, nil
}

func sliceStringMin(s []interface{}, min string) (string, error) {
	for _, v := range s {
		if v == nil {
			continue
		}
		if vs, ok := v.(string); ok {
			if vs < min {
				min = vs
			}
		} else if v != nil {
			return "", fmt.Errorf("requires string but found %[1]T(%[1]v)", v)
		}
	}
	return min, nil
}

func dedup(r []interface{}, col []interface{}, all bool) (interface{}, error) {
	keyset := make(map[string]bool)
	result := make([]interface{}, 0)
	for i, m := range col {
		key := fmt.Sprintf("%v", m)
		if _, ok := keyset[key]; !ok {
			if all {
				result = append(result, r[i])
			} else if i == len(col)-1 {
				result = append(result, r[i])
			}
			keyset[key] = true
		}
	}
	if !all {
		if len(result) == 0 {
			return nil, nil
		} else {
			return result[0], nil
		}
	} else {
		return result, nil
	}
}
