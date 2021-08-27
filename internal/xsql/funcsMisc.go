// Copyright 2021 EMQ Technologies Co., Ltd.
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

package xsql

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/PaesslerAG/gval"
	"github.com/PaesslerAG/jsonpath"
	"github.com/google/uuid"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"hash"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
)

func convCall(name string, args []interface{}) (interface{}, bool) {
	switch name {
	case "cast":
		if v, ok := args[1].(string); ok {
			v = strings.ToLower(v)
			switch v {
			case "bigint":
				if v1, ok1 := args[0].(int); ok1 {
					return v1, true
				} else if v1, ok1 := args[0].(float64); ok1 {
					return int(v1), true
				} else if v1, ok1 := args[0].(string); ok1 {
					if temp, err := strconv.Atoi(v1); err == nil {
						return temp, true
					} else {
						return err, false
					}
				} else if v1, ok1 := args[0].(bool); ok1 {
					if v1 {
						return 1, true
					} else {
						return 0, true
					}
				} else {
					return fmt.Errorf("Not supported type conversion."), false
				}
			case "float":
				if v1, ok1 := args[0].(int); ok1 {
					return float64(v1), true
				} else if v1, ok1 := args[0].(float64); ok1 {
					return v1, true
				} else if v1, ok1 := args[0].(string); ok1 {
					if temp, err := strconv.ParseFloat(v1, 64); err == nil {
						return temp, true
					} else {
						return err, false
					}
				} else if v1, ok1 := args[0].(bool); ok1 {
					if v1 {
						return 1.0, true
					} else {
						return 0.0, true
					}
				} else {
					return fmt.Errorf("Not supported type conversion."), false
				}
			case "string":
				if v1, ok1 := args[0].(int); ok1 {
					return fmt.Sprintf("%d", v1), true
				} else if v1, ok1 := args[0].(float64); ok1 {
					return fmt.Sprintf("%g", v1), true
				} else if v1, ok1 := args[0].(string); ok1 {
					return v1, true
				} else if v1, ok1 := args[0].(bool); ok1 {
					if v1 {
						return "true", true
					} else {
						return "false", true
					}
				} else {
					return fmt.Errorf("Not supported type conversion."), false
				}
			case "boolean":
				if v1, ok1 := args[0].(int); ok1 {
					if v1 == 0 {
						return false, true
					} else {
						return true, true
					}
				} else if v1, ok1 := args[0].(float64); ok1 {
					if v1 == 0.0 {
						return false, true
					} else {
						return true, true
					}
				} else if v1, ok1 := args[0].(string); ok1 {
					if temp, err := strconv.ParseBool(v1); err == nil {
						return temp, true
					} else {
						return err, false
					}
				} else if v1, ok1 := args[0].(bool); ok1 {
					return v1, true
				} else {
					return fmt.Errorf("Not supported type conversion."), false
				}
			case "datetime":
				dt, err := cast.InterfaceToTime(args[0], "")
				if err != nil {
					return err, false
				} else {
					return dt, true
				}
			default:
				return fmt.Errorf("Unknow type, only support bigint, float, string, boolean and datetime."), false
			}
		} else {
			return fmt.Errorf("Expect string type for the 2nd parameter."), false
		}
	case "chr":
		if v, ok := args[0].(int); ok {
			return rune(v), true
		} else if v, ok := args[0].(float64); ok {
			temp := int(v)
			return rune(temp), true
		} else if v, ok := args[0].(string); ok {
			if len(v) > 1 {
				return fmt.Errorf("Parameter length cannot larger than 1."), false
			}
			r := []rune(v)
			return r[0], true
		} else {
			return fmt.Errorf("Only bigint, float and string type can be convert to char type."), false
		}
	case "encode":
		if v, ok := args[1].(string); ok {
			v = strings.ToLower(v)
			if v == "base64" {
				if v1, ok1 := args[0].(string); ok1 {
					return b64.StdEncoding.EncodeToString([]byte(v1)), true
				} else {
					return fmt.Errorf("Only string type can be encoded."), false
				}
			} else {
				return fmt.Errorf("Only base64 encoding is supported."), false
			}
		}
	case "trunc":
		var v0 float64
		if v1, ok := args[0].(int); ok {
			v0 = float64(v1)
		} else if v1, ok := args[0].(float64); ok {
			v0 = v1
		} else {
			return fmt.Errorf("Only int and float type can be truncated."), false
		}
		if v2, ok := args[1].(int); ok {
			return toFixed(v0, v2), true
		} else {
			return fmt.Errorf("The 2nd parameter must be int value."), false
		}
	default:
		return fmt.Errorf("Not supported function name %s", name), false
	}
	return nil, false
}

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

func toFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

func hashCall(name string, args []interface{}) (interface{}, bool) {
	if args[0] == nil {
		return nil, true
	}
	arg0 := cast.ToStringAlways(args[0])
	var h hash.Hash
	switch name {
	case "md5":
		h = md5.New()
	case "sha1":
		h = sha1.New()
	case "sha256":
		h = sha256.New()
	case "sha384":
		h = sha512.New384()
	case "sha512":
		h = sha512.New()
	default:
		return fmt.Errorf("unknown hash function name %s", name), false
	}
	_, err := io.WriteString(h, arg0)
	if err != nil {
		return err, false
	}
	return fmt.Sprintf("%x", h.Sum(nil)), true
}

func otherCall(name string, args []interface{}) (interface{}, bool) {
	switch name {
	case "isnull":
		if args[0] == nil {
			return true, true
		} else {
			v := reflect.ValueOf(args[0])
			switch v.Kind() {
			case reflect.Slice, reflect.Map:
				return v.IsNil(), true
			default:
				return false, true
			}
		}
	case "newuuid":
		if newUUID, err := uuid.NewUUID(); err != nil {
			return err, false
		} else {
			return newUUID.String(), true
		}
	case "tstamp":
		return conf.GetNowInMilli(), true
	case "mqtt":
		if v, ok := args[0].(string); ok {
			return v, true
		}
		return nil, false
	case "meta":
		return args[0], true
	case "cardinality":
		val := reflect.ValueOf(args[0])
		if val.Kind() == reflect.Slice {
			return val.Len(), true
		}
		return 0, true
	default:
		return fmt.Errorf("unknown function name %s", name), false
	}
}

func jsonCall(name string, args []interface{}) (interface{}, bool) {
	var input interface{}
	at := reflect.TypeOf(args[0])
	if at != nil {
		switch at.Kind() {
		case reflect.Map:
			input = convertToInterfaceArr(args[0].(map[string]interface{}))
		case reflect.Slice:
			input = convertSlice(args[0])
		case reflect.String:
			v, _ := args[0].(string)
			err := json.Unmarshal([]byte(v), &input)
			if err != nil {
				return fmt.Errorf("%s function error: the first argument '%v' is not a valid json string", name, args[0]), false
			}
		default:
			return fmt.Errorf("%s function error: the first argument must be a map but got %v", name, args[0]), false
		}
	} else {
		return fmt.Errorf("%s function error: the first argument must be a map but got nil", name), false
	}

	builder := gval.Full(jsonpath.PlaceholderExtension())
	path, err := builder.NewEvaluable(args[1].(string))
	if err != nil {
		return fmt.Errorf("%s function error: %s", name, err), false
	}
	result, err := path(context.Background(), input)
	if err != nil {
		if name == "json_path_exists" {
			return false, true
		}
		return fmt.Errorf("%s function error: %s", name, err), false
	}
	switch name {
	case "json_path_query":
		return result, true
	case "json_path_query_first":
		if arr, ok := result.([]interface{}); ok {
			return arr[0], true
		} else {
			return fmt.Errorf("%s function error: query result (%v) is not an array", name, result), false
		}
	case "json_path_exists":
		if result == nil {
			return false, true
		}
		e := true
		switch reflect.TypeOf(result).Kind() {
		case reflect.Slice, reflect.Array:
			e = reflect.ValueOf(result).Len() > 0
		default:
			e = result != nil
		}
		return e, true
	}
	return fmt.Errorf("invalid function name: %s", name), false
}

func convertToInterfaceArr(orig map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range orig {
		vt := reflect.TypeOf(v)
		if vt == nil {
			result[k] = nil
			continue
		}
		switch vt.Kind() {
		case reflect.Slice:
			result[k] = convertSlice(v)
		case reflect.Map:
			result[k] = convertToInterfaceArr(v.(map[string]interface{}))
		default:
			result[k] = v
		}
	}
	return result
}

func convertSlice(v interface{}) []interface{} {
	value := reflect.ValueOf(v)
	tempArr := make([]interface{}, value.Len())
	for i := 0; i < value.Len(); i++ {
		item := value.Index(i)
		if item.Kind() == reflect.Map {
			tempArr[i] = convertToInterfaceArr(item.Interface().(map[string]interface{}))
		} else if item.Kind() == reflect.Slice {
			tempArr[i] = convertSlice(item.Interface())
		} else {
			tempArr[i] = item.Interface()
		}
	}
	return tempArr
}
