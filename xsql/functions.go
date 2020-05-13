package xsql

import (
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/plugins"
	"github.com/emqx/kuiper/xstream/api"
	"strings"
)

type FunctionValuer struct {
	plugins map[string]api.Function
}

func (*FunctionValuer) Value(key string) (interface{}, bool) {
	return nil, false
}

func (*FunctionValuer) Meta(key string) (interface{}, bool) {
	return nil, false
}

var aggFuncMap = map[string]string{"avg": "",
	"count": "",
	"max":   "", "min": "",
	"sum": "",
}

var mathFuncMap = map[string]string{"abs": "", "acos": "", "asin": "", "atan": "", "atan2": "",
	"bitand": "", "bitor": "", "bitxor": "", "bitnot": "",
	"ceil": "", "cos": "", "cosh": "",
	"exp": "",
	"ln":  "", "log": "",
	"mod":   "",
	"power": "",
	"rand":  "", "round": "",
	"sign": "", "sin": "", "sinh": "", "sqrt": "",
	"tan": "", "tanh": "",
}

var strFuncMap = map[string]string{"concat": "",
	"endswith":    "",
	"format_time": "",
	"indexof":     "",
	"length":      "", "lower": "", "lpad": "", "ltrim": "",
	"numbytes":       "",
	"regexp_matches": "", "regexp_replace": "", "regexp_substr": "", "rpad": "", "rtrim": "",
	"substring": "", "startswith": "", "split_value": "",
	"trim":  "",
	"upper": "",
}

var convFuncMap = map[string]string{"concat": "", "cast": "", "chr": "",
	"encode": "",
	"trunc":  "",
}

var hashFuncMap = map[string]string{"md5": "",
	"sha1": "", "sha256": "", "sha384": "", "sha512": "",
}

var otherFuncMap = map[string]string{"isnull": "",
	"newuuid": "", "timestamp": "", "mqtt": "", "meta": "",
}

func (fv *FunctionValuer) Call(name string, args []interface{}) (interface{}, bool) {
	lowerName := strings.ToLower(name)
	if _, ok := mathFuncMap[lowerName]; ok {
		return mathCall(name, args)
	} else if _, ok := strFuncMap[lowerName]; ok {
		return strCall(lowerName, args)
	} else if _, ok := convFuncMap[lowerName]; ok {
		return convCall(lowerName, args)
	} else if _, ok := hashFuncMap[lowerName]; ok {
		return hashCall(lowerName, args)
	} else if _, ok := otherFuncMap[lowerName]; ok {
		return otherCall(lowerName, args)
	} else if _, ok := aggFuncMap[lowerName]; ok {
		return nil, false
	} else {
		common.Log.Debugf("run func %s", name)
		var (
			nf  api.Function
			ok  bool
			err error
		)
		if nf, ok = fv.plugins[name]; !ok {
			nf, err = plugins.GetFunction(name)
			if err != nil {
				return err, false
			}
			fv.plugins[name] = nf
		}
		if nf.IsAggregate() {
			return nil, false
		}
		result, ok := nf.Exec(args)
		common.Log.Debugf("run custom function %s, get result %v", name, result)
		return result, ok
	}
}
