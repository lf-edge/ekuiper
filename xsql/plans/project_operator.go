package plans

import (
	"context"
	"encoding/json"
	"engine/xsql"
	"fmt"
	"strconv"
	"strings"
)

type ProjectPlan struct {
	Fields xsql.Fields
}

func (pp *ProjectPlan) Apply(ctx context.Context, data interface{}) interface{} {
	var input map[string]interface{}
	if d, ok := data.(map[string]interface{}); !ok {
		fmt.Printf("Expect map[string]interface{} type.\n")
		return nil
	} else {
		input = d
	}

	var result = make(map[string]interface{})

	ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(xsql.MapValuer(input), &xsql.FunctionValuer{}, &xsql.WildcardValuer{Data: input})}

	for _, f := range pp.Fields {
		v := ve.Eval(f.Expr)
		if val, ok := v.(map[string]interface{}); ok { //It should be the asterisk in fields list.
			result = val
			break
		} else {
			result[assignName(f.Name, f.AName, result)] = v
		}
	}

	if ret, err := json.Marshal(result); err == nil {
		return ret
	} else {
		fmt.Printf("Found error: %v.\n", err)
		return nil
	}
}

const DEFAULT_FIELD_NAME_PREFIX string = "rengine_field_"

func assignName(name, alias string, fields map[string] interface{}) string {
	if result := strings.Trim(alias, " "); result != "" {
		return result
	}

	if result := strings.Trim(name, " "); result != "" {
		return result
	}

	for i := 0; i < 2048; i++ {
		key := DEFAULT_FIELD_NAME_PREFIX + strconv.Itoa(i)
		if _, ok := fields[key]; !ok {
			return key
		}
	}
	fmt.Printf("Cannot assign a default field name.\n")
	return ""
}