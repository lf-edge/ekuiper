package ast

import (
	"github.com/emqx/kuiper/pkg/api"
	"strings"
	"sync"
)

type FuncType int

const (
	NotFoundFunc FuncType = iota - 1
	AggFunc
	MathFunc
	StrFunc
	ConvFunc
	HashFunc
	JsonFunc
	OtherFunc
)

var maps = []map[string]string{
	aggFuncMap, mathFuncMap, strFuncMap, convFuncMap, hashFuncMap, jsonFuncMap, otherFuncMap,
}

var aggFuncMap = map[string]string{"avg": "",
	"count": "",
	"max":   "", "min": "",
	"sum":          "",
	"collect":      "",
	"deduplicate":  "",
	"window_start": "",
	"window_end":   "",
}

var funcWithAsteriskSupportMap = map[string]string{
	"collect": "",
	"count":   "",
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

var jsonFuncMap = map[string]string{
	"json_path_query": "", "json_path_query_first": "", "json_path_exists": "",
}

var otherFuncMap = map[string]string{"isnull": "",
	"newuuid": "", "tstamp": "", "mqtt": "", "meta": "", "cardinality": "",
}

type FuncRuntime interface {
	Get(name string) (api.Function, api.FunctionContext, error)
}

var (
	once sync.Once
	ff   *FuncFinder
)

// FuncFinder Singleton, must be initiated when starting
type FuncFinder struct {
	runtime FuncRuntime
}

// InitFuncFinder must be called when starting
func InitFuncFinder(runtime FuncRuntime) {
	once.Do(func() {
		ff = &FuncFinder{runtime: runtime}
	})
	ff.runtime = runtime
}

// FuncFinderSingleton must be inited before calling this
func FuncFinderSingleton() *FuncFinder {
	return ff
}

func (ff *FuncFinder) IsAggFunc(f *Call) bool {
	fn := strings.ToLower(f.Name)
	if _, ok := aggFuncMap[fn]; ok {
		return true
	} else if _, ok := strFuncMap[fn]; ok {
		return false
	} else if _, ok := convFuncMap[fn]; ok {
		return false
	} else if _, ok := hashFuncMap[fn]; ok {
		return false
	} else if _, ok := otherFuncMap[fn]; ok {
		return false
	} else if _, ok := mathFuncMap[fn]; ok {
		return false
	} else {
		if nf, _, err := ff.runtime.Get(f.Name); err == nil {
			if nf.IsAggregate() {
				//Add cache
				aggFuncMap[fn] = ""
				return true
			}
		}
	}
	return false
}

func (ff *FuncFinder) FuncType(name string) FuncType {
	for i, m := range maps {
		if _, ok := m[strings.ToLower(name)]; ok {
			return FuncType(i)
		}
	}
	return NotFoundFunc
}
