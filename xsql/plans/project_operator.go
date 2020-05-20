package plans

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"strconv"
	"strings"
)

type ProjectPlan struct {
	Fields      xsql.Fields
	IsAggregate bool

	isTest bool
}

/**
 *  input: *xsql.Tuple from preprocessor or filterOp | xsql.WindowTuplesSet from windowOp or filterOp | xsql.JoinTupleSets from joinOp or filterOp
 *  output: []map[string]interface{}
 */
func (pp *ProjectPlan) Apply(ctx api.StreamContext, data interface{}) interface{} {
	log := ctx.GetLogger()
	log.Debugf("project plan receive %s", data)
	var results []map[string]interface{}
	switch input := data.(type) {
	case error:
		return input
	case *xsql.Tuple:
		okeys := input.OriginalKeys
		ve := pp.getVE(input, input)
		if r, err := project(pp.Fields, ve, okeys, pp.isTest); err != nil {
			return fmt.Errorf("run Select error: %s", err)
		} else {
			results = append(results, r)
		}
	case xsql.WindowTuplesSet:
		if len(input) != 1 {
			return fmt.Errorf("run Select error: the input WindowTuplesSet with multiple tuples cannot be evaluated)")
		}
		ms := input[0].Tuples
		for _, v := range ms {
			ve := pp.getVE(&v, input)
			if r, err := project(pp.Fields, ve, nil, pp.isTest); err != nil {
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
			ve := pp.getVE(&v, input)
			if r, err := project(pp.Fields, ve, nil, pp.isTest); err != nil {
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
			ve := pp.getVE(v[0], v)
			if r, err := project(pp.Fields, ve, nil, pp.isTest); err != nil {
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

func (pp *ProjectPlan) getVE(tuple xsql.DataValuer, agg xsql.AggregateData) *xsql.ValuerEval {
	if pp.IsAggregate {
		return &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(agg, tuple, &xsql.FunctionValuer{}, &xsql.AggregateFunctionValuer{Data: agg}, &xsql.WildcardValuer{Data: tuple})}
	} else {
		return &xsql.ValuerEval{Valuer: xsql.MultiValuer(tuple, &xsql.FunctionValuer{}, &xsql.WildcardValuer{Data: tuple})}
	}
}

func project(fs xsql.Fields, ve *xsql.ValuerEval, okeys xsql.OriginalKeys, isTest bool) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for _, f := range fs {
		//Avoid to re-evaluate for non-agg field has alias name, which was already evaluated in pre-processor operator.
		if f.AName != "" && (!xsql.HasAggFuncs(f.Expr)) && !isTest {
			fr := &xsql.FieldRef{StreamName: "", Name: f.AName}
			v := ve.Eval(fr)
			if e, ok := v.(error); ok {
				return nil, e
			}
			result[f.AName] = v
		} else {
			v := ve.Eval(f.Expr)
			if e, ok := v.(error); ok {
				return nil, e
			}
			if _, ok := f.Expr.(*xsql.Wildcard); ok || f.Name == "*" {
				switch val := v.(type) {
				case map[string]interface{}:
					for k, v := range val {
						if _, ok := result[k]; !ok {
							if ok, okey := xsql.GetOriginalKey(k, okeys); ok {
								result[okey] = v
							} else {
								result[k] = v
							}
						}
					}
				case xsql.Message:
					for k, v := range val {
						if ok, okey := xsql.GetOriginalKey(k, okeys); ok {
							result[okey] = v
						} else {
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
	}
	return result, nil
}



const DEFAULT_FIELD_NAME_PREFIX string = "rengine_field_"

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
