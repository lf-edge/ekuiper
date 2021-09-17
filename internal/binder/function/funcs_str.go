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

package function

import (
	"bytes"
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

func strCall(name string, args []interface{}) (interface{}, bool) {
	switch name {
	case "concat":
		var b bytes.Buffer
		for _, arg := range args {
			b.WriteString(cast.ToStringAlways(arg))
		}
		return b.String(), true
	case "endswith":
		if args[0] == nil || args[1] == nil {
			return false, true
		}
		arg0, arg1 := cast.ToStringAlways(args[0]), cast.ToStringAlways(args[1])
		return strings.HasSuffix(arg0, arg1), true
	case "indexof":
		if args[0] == nil || args[1] == nil {
			return -1, true
		}
		arg0, arg1 := cast.ToStringAlways(args[0]), cast.ToStringAlways(args[1])
		return strings.Index(arg0, arg1), true
	case "length":
		arg0 := cast.ToStringAlways(args[0])
		return utf8.RuneCountInString(arg0), true
	case "lower":
		if args[0] == nil {
			return nil, true
		}
		arg0 := cast.ToStringAlways(args[0])
		return strings.ToLower(arg0), true
	case "lpad":
		if args[0] == nil {
			return nil, true
		}
		arg0 := cast.ToStringAlways(args[0])
		arg1, err := cast.ToInt(args[1], cast.STRICT)
		if err != nil {
			return err, false
		}
		return strings.Repeat(" ", arg1) + arg0, true
	case "ltrim":
		if args[0] == nil {
			return nil, true
		}
		arg0 := cast.ToStringAlways(args[0])
		return strings.TrimLeftFunc(arg0, unicode.IsSpace), true
	case "numbytes":
		arg0 := cast.ToStringAlways(args[0])
		return len(arg0), true
	case "format_time":
		if args[0] == nil {
			return nil, true
		}
		arg0, err := cast.InterfaceToTime(args[0], "")
		if err != nil {
			return err, false
		}
		arg1 := cast.ToStringAlways(args[1])
		if s, err := cast.FormatTime(arg0, arg1); err == nil {
			return s, true
		} else {
			return err, false
		}
	case "regexp_matches":
		if args[0] == nil || args[1] == nil {
			return false, true
		}
		arg0, arg1 := cast.ToStringAlways(args[0]), cast.ToStringAlways(args[1])
		if matched, err := regexp.MatchString(arg1, arg0); err != nil {
			return err, false
		} else {
			return matched, true
		}
	case "regexp_replace":
		if args[0] == nil || args[1] == nil || args[2] == nil {
			return nil, true
		}
		arg0, arg1, arg2 := cast.ToStringAlways(args[0]), cast.ToStringAlways(args[1]), cast.ToStringAlways(args[2])
		if re, err := regexp.Compile(arg1); err != nil {
			return err, false
		} else {
			return re.ReplaceAllString(arg0, arg2), true
		}
	case "regexp_substr":
		if args[0] == nil || args[1] == nil {
			return nil, true
		}
		arg0, arg1 := cast.ToStringAlways(args[0]), cast.ToStringAlways(args[1])
		if re, err := regexp.Compile(arg1); err != nil {
			return err, false
		} else {
			return re.FindString(arg0), true
		}
	case "rpad":
		if args[0] == nil {
			return nil, true
		}
		arg0 := cast.ToStringAlways(args[0])
		arg1, err := cast.ToInt(args[1], cast.STRICT)
		if err != nil {
			return err, false
		}
		return arg0 + strings.Repeat(" ", arg1), true
	case "rtrim":
		if args[0] == nil {
			return nil, true
		}
		arg0 := cast.ToStringAlways(args[0])
		return strings.TrimRightFunc(arg0, unicode.IsSpace), true
	case "substring":
		if args[0] == nil {
			return nil, true
		}
		arg0 := cast.ToStringAlways(args[0])
		arg1, err := cast.ToInt(args[1], cast.STRICT)
		if err != nil {
			return err, false
		}
		if arg1 < 0 {
			return fmt.Errorf("start index must be a positive number"), false
		}
		if len(args) > 2 {
			arg2, err := cast.ToInt(args[2], cast.STRICT)
			if err != nil {
				return err, false
			}
			if arg2 < 0 {
				return fmt.Errorf("end index must be a positive number"), false
			}
			if arg1 > arg2 {
				return fmt.Errorf("start index must be smaller than end index"), false
			}
			if arg1 > len(arg0) {
				return "", true
			}
			if arg2 > len(arg0) {
				return arg0[arg1:], true
			}
			return arg0[arg1:arg2], true
		} else {
			if arg1 > len(arg0) {
				return "", true
			}
			return arg0[arg1:], true
		}
	case "startswith":
		if args[0] == nil {
			return false, true
		}
		arg0, arg1 := cast.ToStringAlways(args[0]), cast.ToStringAlways(args[1])
		return strings.HasPrefix(arg0, arg1), true
	case "split_value":
		if args[0] == nil || args[1] == nil {
			return nil, true
		}
		arg0, arg1 := cast.ToStringAlways(args[0]), cast.ToStringAlways(args[1])
		ss := strings.Split(arg0, arg1)
		v, _ := cast.ToInt(args[2], cast.STRICT)
		if v > (len(ss) - 1) {
			return fmt.Errorf("%d out of index array (size = %d)", v, len(ss)), false
		} else {
			return ss[v], true
		}
	case "trim":
		if args[0] == nil {
			return nil, true
		}
		arg0 := cast.ToStringAlways(args[0])
		return strings.TrimSpace(arg0), true
	case "upper":
		if args[0] == nil {
			return nil, true
		}
		arg0 := cast.ToStringAlways(args[0])
		return strings.ToUpper(arg0), true
	default:
		return fmt.Errorf("unknown string function name %s", name), false
	}
}
