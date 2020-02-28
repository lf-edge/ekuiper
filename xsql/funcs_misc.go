package xsql

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	b64 "encoding/base64"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/google/uuid"
	"hash"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
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
					return string(v1), true
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
				return fmt.Errorf("Not supported type conversion."), false
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
	arg0 := common.ToString(args[0])
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
	io.WriteString(h, arg0)
	return fmt.Sprintf("%x", h.Sum(nil)), true
}

func otherCall(name string, args []interface{}) (interface{}, bool) {
	switch name {
	case "isnull":
		return args[0] == nil, true
	case "newuuid":
		if uuid, err := uuid.NewUUID(); err != nil {
			return err, false
		} else {
			return uuid.String(), true
		}
	case "timestamp":
		return common.TimeToUnixMilli(time.Now()), true
	case "mqtt":
		if v, ok := args[0].(string); ok {
			return v, true
		}
		return nil, false
	default:
		return fmt.Errorf("unknown function name %s", name), false
	}
}
