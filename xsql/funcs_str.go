package xsql

import (
	"bytes"
	"github.com/emqx/kuiper/common"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

func strCall(name string, args []interface{}) (interface{}, bool) {
	switch name {
	case "concat":
		var b bytes.Buffer
		for _, arg := range args {
			b.WriteString(common.ToString(arg))
		}
		return b.String(), true
	case "endswith":
		arg0, arg1 := common.ToString(args[0]), common.ToString(args[1])
		return strings.HasSuffix(arg0, arg1), true
	case "indexof":
		arg0, arg1 := common.ToString(args[0]), common.ToString(args[1])
		return strings.Index(arg0, arg1), true
	case "length":
		arg0 := common.ToString(args[0])
		return utf8.RuneCountInString(arg0), true
	case "lower":
		arg0 := common.ToString(args[0])
		return strings.ToLower(arg0), true
	case "lpad":
		arg0 := common.ToString(args[0])
		arg1, err := common.ToInt(args[1])
		if err != nil{
			return err, false
		}
		return strings.Repeat(" ", arg1) + arg0, true
	case "ltrim":
		arg0 := common.ToString(args[0])
		return strings.TrimLeftFunc(arg0, unicode.IsSpace), true
	case "numbytes":
		arg0 := common.ToString(args[0])
		return len(arg0), true
	case "format_time":
		arg0 := args[0]
		if t, ok := arg0.(time.Time); ok{
			arg1 := common.ToString(args[1])
			if s, err := common.FormatTime(t, arg1); err==nil{
				return s, true
			}
		}
		return "", false
	case "regexp_matches":
		arg0, arg1 := common.ToString(args[0]), common.ToString(args[1])
		if matched, err := regexp.MatchString(arg1, arg0); err != nil{
			return err, false
		}else{
			return matched, true
		}
	case "regexp_replace":
		arg0, arg1, arg2 := common.ToString(args[0]), common.ToString(args[1]), common.ToString(args[2])
		if re, err := regexp.Compile(arg1); err != nil{
			return err, false
		}else{
			return re.ReplaceAllString(arg0, arg2), true
		}
	case "regexp_substr":
		arg0, arg1 := common.ToString(args[0]), common.ToString(args[1])
		if re, err := regexp.Compile(arg1); err != nil{
			return err, false
		}else{
			return re.FindString(arg0), true
		}
	case "rpad":
		arg0 := common.ToString(args[0])
		arg1, err := common.ToInt(args[1])
		if err != nil{
			return err, false
		}
		return arg0 + strings.Repeat(" ", arg1), true
	case "rtrim":
		arg0 := common.ToString(args[0])
		return strings.TrimRightFunc(arg0, unicode.IsSpace), true
	case "substring":
		arg0 := common.ToString(args[0])
		arg1, err := common.ToInt(args[1])
		if err != nil{
			return err, false
		}
		if len(args) > 2{
			arg2, err := common.ToInt(args[2])
			if err != nil{
				return err, false
			}
			return arg0[arg1:arg2], true
		}else{
			return arg0[arg1:], true
		}
	case "startswith":
		arg0, arg1 := common.ToString(args[0]), common.ToString(args[1])
		return strings.HasPrefix(arg0, arg1), true
	case "split_value":
		arg0, arg1 := common.ToString(args[0]), common.ToString(args[1])
		ss := strings.Split(arg0, arg1)
		v, _ := common.ToInt(args[2])
		if v > (len(ss) - 1) {
			return fmt.Errorf("%d out of index array (size = %d)", v, len(ss)), false
		} else {
			return ss[v], true
		}
	case "trim":
		arg0 := common.ToString(args[0])
		return strings.TrimSpace(arg0), true
	case "upper":
		arg0 := common.ToString(args[0])
		return strings.ToUpper(arg0), true
	default:
		return fmt.Errorf("unknown string function name %s", name), false
	}
}

