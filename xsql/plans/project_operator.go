package plans

import (
	"encoding/json"
	"engine/xsql"
	"engine/xstream/api"
	"fmt"
	"strconv"
	"strings"
)

type ProjectPlan struct {
	Fields xsql.Fields
	IsAggregate bool
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
	case *xsql.Tuple:
		ve := pp.getVE(input, input)
		results = append(results, project(pp.Fields, ve))
	case xsql.WindowTuplesSet:
		if len(input) != 1 {
			log.Infof("WindowTuplesSet with multiple tuples cannot be evaluated")
			return nil
		}
		ms := input[0].Tuples
		for _, v := range ms {
			ve := pp.getVE(&v, input)
			results = append(results, project(pp.Fields, ve))
			if pp.IsAggregate{
				break
			}
		}
	case xsql.JoinTupleSets:
		ms := input
		for _, v := range ms {
			ve := pp.getVE(&v, input)
			results = append(results, project(pp.Fields, ve))
			if pp.IsAggregate{
				break
			}
		}
	case xsql.GroupedTuplesSet:
		for _, v := range input{
			ve := pp.getVE(v[0], v)
			results = append(results, project(pp.Fields, ve))
		}
	default:
		log.Errorf("Expect xsql.Valuer or its array type")
		return nil
	}

	if ret, err := json.Marshal(results); err == nil {
		return ret
	} else {
		fmt.Printf("Found error: %v", err)
		return nil
	}
}

func (pp *ProjectPlan) getVE(tuple xsql.DataValuer, agg xsql.AggregateData) *xsql.ValuerEval{
	if pp.IsAggregate{
		return &xsql.ValuerEval{Valuer: xsql.MultiAggregateValuer(agg, tuple, &xsql.FunctionValuer{}, &xsql.AggregateFunctionValuer{Data: agg}, &xsql.WildcardValuer{Data: tuple})}
	}else{
		return &xsql.ValuerEval{Valuer: xsql.MultiValuer(tuple, &xsql.FunctionValuer{}, &xsql.WildcardValuer{Data: tuple})}
	}
}

func project(fs xsql.Fields, ve *xsql.ValuerEval) map[string]interface{} {
	result := make(map[string]interface{})
	for _, f := range fs {
		//Avoid to re-evaluate for non-agg field has alias name, which was already evaluated in pre-processor operator.
		if f.AName != "" && (!xsql.HasAggFuncs(f.Expr)){
			fr := &xsql.FieldRef{StreamName:"", Name:f.AName}
			v := ve.Eval(fr);
			result[f.AName] = v
		} else {
			v := ve.Eval(f.Expr)
			if _, ok := f.Expr.(*xsql.Wildcard); ok || f.Name == "*"{
				switch val := v.(type) {
				case map[string]interface{} :
					for k, v := range val{
						if _, ok := result[k]; !ok{
							result[k] = v
						}
					}
				case xsql.Message:
					for k, v := range val{
						if _, ok := result[k]; !ok{
							result[k] = v
						}
					}
				default:
					fmt.Printf("Wildcarder does not return map")
				}
			} else {
				if v != nil {
					n := assignName(f.Name, f.AName, result)
					if _, ok := result[n]; !ok{
						result[n] = v
					}
				}
			}
		}
	}
	return result
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
	fmt.Printf("Cannot assign a default field name")
	return ""
}