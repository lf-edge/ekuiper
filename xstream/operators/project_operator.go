package operators

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"strconv"
	"strings"
)

type ProjectOp struct {
	Fields      xsql.Fields
	IsAggregate bool
	SendMeta    bool
	isTest      bool
}

/**
 *  input: *xsql.Tuple from preprocessor or filterOp | xsql.WindowTuplesSet from windowOp or filterOp | xsql.JoinTupleSets from joinOp or filterOp
 *  output: []map[string]interface{}
 */
func (pp *ProjectOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) interface{} {
	log := ctx.GetLogger()
	log.Debugf("project plan receive %s", data)
	var results []map[string]interface{}
	switch input := data.(type) {
	case error:
		return input
	case *xsql.Tuple:
		ve := pp.getVE(input, input, fv, afv)
		if r, err := project(pp.Fields, ve, pp.isTest); err != nil {
			return fmt.Errorf("run Select error: %s", err)
		} else {
			if pp.SendMeta && input.Metadata != nil {
				r[common.MetaKey] = input.Metadata
			}
			results = append(results, r)
		}
	case xsql.WindowTuplesSet:
		if len(input) != 1 {
			return fmt.Errorf("run Select error: the input WindowTuplesSet with multiple tuples cannot be evaluated)")
		}
		ms := input[0].Tuples
		for _, v := range ms {
			ve := pp.getVE(&v, input, fv, afv)
			if r, err := project(pp.Fields, ve, pp.isTest); err != nil {
				return fmt.Errorf("run Select error: %s", err)
			} else {
				results = append(results, r)
			}
			if pp.IsAggregate {
				break
			}
		}
	case xsql.JoinTupleSets:
		ms := input
		for _, v := range ms {
			ve := pp.getVE(&v, input, fv, afv)
			if r, err := project(pp.Fields, ve, pp.isTest); err != nil {
				return err
			} else {
				results = append(results, r)
			}
			if pp.IsAggregate {
				break
			}
		}
	case xsql.GroupedTuplesSet:
		for _, v := range input {
			ve := pp.getVE(v[0], v, fv, afv)
			if r, err := project(pp.Fields, ve, pp.isTest); err != nil {
				return fmt.Errorf("run Select error: %s", err)
			} else {
				results = append(results, r)
			}
		}
	default:
		return fmt.Errorf("run Select error: invalid input %[1]T(%[1]v)", input)
	}

	if ret, err := json.Marshal(results); err == nil {
		return ret
	} else {
		return fmt.Errorf("run Select error: %v", err)
	}
}

func (pp *ProjectOp) getVE(tuple xsql.DataValuer, agg xsql.AggregateData, fv *xsql.FunctionValuer, afv *xsql.AggregateFunctionValuer) *xsql.ValuerEval {
	afv.SetData(agg)
	if pp.IsAggregate {
		return &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(agg, fv, tuple, fv, afv, &xsql.WildcardValuer{Data: tuple})}
	} else {
		return &xsql.ValuerEval{Valuer: xsql.MultiValuer(tuple, fv, &xsql.WildcardValuer{Data: tuple})}
	}
}

func project(fs xsql.Fields, ve *xsql.ValuerEval, isTest bool) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for _, f := range fs {
		expr := f.Expr
		//Avoid to re-evaluate for non-agg field has alias name, which was already evaluated in pre-processor operator.
		if f.AName != "" && !isTest {
			expr = &xsql.FieldRef{StreamName: xsql.DEFAULT_STREAM, Name: f.AName}
		}
		v := ve.Eval(expr)
		if e, ok := v.(error); ok {
			return nil, e
		}
		if _, ok := f.Expr.(*xsql.Wildcard); ok || f.Name == "*" {
			switch val := v.(type) {
			case map[string]interface{}:
				for k, v := range val {
					if _, ok := result[k]; !ok {
						result[k] = v
					}
				}
			case xsql.Message:
				for k, v := range val {
					if _, ok := result[k]; !ok {
						result[k] = v
					}
				}
			default:
				return nil, fmt.Errorf("wildcarder does not return map")
			}
		} else {
			if v != nil {
				n := assignName(f.Name, f.AName, result)
				if _, ok := result[n]; !ok {
					result[n] = v
				}
			}
		}
	}
	return result, nil
}

const DEFAULT_FIELD_NAME_PREFIX string = "kuiper_field_"

func assignName(name, alias string, fields map[string]interface{}) string {
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
	fmt.Printf("Cannot assign a default field name")
	return ""
}
