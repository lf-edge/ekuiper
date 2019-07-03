package plans

import (
	"context"
	"encoding/json"
	"engine/common"
	"engine/xsql"
	"errors"
	"fmt"
	"reflect"
)

type Preprocessor struct {
	StreamStmt *xsql.StreamStmt
}

// data is a json string
func (p *Preprocessor) Apply(ctx context.Context, data interface{}) interface{} {
	log := common.Log
	tuple, ok := data.(*xsql.Tuple)
	if !ok {
		log.Errorf("Expect tuple data type.\n")
		return nil
	}
	var jsonData []byte
	if d, ok := tuple.Message.([]byte); !ok {
		log.Errorf("Expect byte array data type.\n")
		return nil
	} else {
		jsonData = d
	}
	log.Infof("preprocessor receive %s", jsonData)
	//The unmarshal type can only be bool, float64, string, []interface{}, map[string]interface{}, nil
	jsonResult := make(map[string]interface{})

	result := make(map[string]interface{})
	if e := json.Unmarshal(jsonData, &jsonResult); e != nil {
		log.Errorf("parse json string %s error: %s", jsonData, e)
		return nil
	}else{
		for _, f := range p.StreamStmt.StreamFields {
			if e = addRecField(f.FieldType, result, jsonResult, f.Name); e != nil{
				log.Errorf("error in preprocessor: %s", e)
				return nil
			}
		}
		tuple.Message = result
		return tuple
	}
}

func addRecField(ft xsql.FieldType, r map[string]interface{}, j map[string]interface{}, p string) error {
	if t, ok := j[p]; ok {
		v := reflect.ValueOf(t)
		jtype := v.Kind()
		switch st := ft.(type) {
		case *xsql.BasicType:
			switch st.Type {
			case xsql.UNKNOWN:
				return errors.New(fmt.Sprintf("invalid data type unknown defined for %s, please check the stream definition", t))
			case xsql.BIGINT:
				if jtype == reflect.Float64{
					r[p] = int(t.(float64))
				}else{
					return errors.New(fmt.Sprintf("invalid data type for %s, expect bigint but found %s", p, t))
				}
			case xsql.FLOAT:
				if jtype == reflect.Float64{
					r[p] = t.(float64)
				}else{
					return errors.New(fmt.Sprintf("invalid data type for %s, expect float but found %s", p, t))
				}
			case xsql.STRINGS:
				if jtype == reflect.String{
					r[p] = t.(string)
				}else{
					return errors.New(fmt.Sprintf("invalid data type for %s, expect string but found %s", p, t))
				}
			case xsql.DATETIME:
				return errors.New(fmt.Sprintf("invalid data type for %s, datetime type is not supported yet", p))
			case xsql.BOOLEAN:
				if jtype == reflect.Bool{
					r[p] = t.(bool)
				}else{
					return errors.New(fmt.Sprintf("invalid data type for %s, expect boolean but found %s", p, t))
				}
			default:
				return errors.New(fmt.Sprintf("invalid data type for %s, it is not supported yet", st))
			}
		case *xsql.ArrayType:
			if jtype != reflect.Slice{
				return errors.New(fmt.Sprintf("invalid data type for %s, expect array but found %s", p, t))
			}
			if tempArr, err := addArrayField(st, t.([]interface{})); err !=nil{
				return err
			}else {
				r[p] = tempArr
			}
		case *xsql.RecType:
			if jtype != reflect.Map{
				return errors.New(fmt.Sprintf("invalid data type for %s, expect struct but found %s", p, t))
			}
			nextJ, ok := j[p].(map[string]interface{})
			if !ok {
				return errors.New(fmt.Sprintf("invalid data type for %s, expect map but found %s", p, t))
			}
			nextR := make(map[string]interface{})
			for _, nextF := range st.StreamFields {
				nextP := nextF.Name
				if e := addRecField(nextF.FieldType, nextR, nextJ, nextP); e != nil{
					return e
				}
			}
			r[p] = nextR
		default:
			return errors.New(fmt.Sprintf("unsupported type %T", st))
		}
		return nil
	}else{
		return errors.New(fmt.Sprintf("invalid data %s, field %s not found", j, p))
	}
}

//ft must be xsql.ArrayType
//side effect: r[p] will be set to the new array
func addArrayField(ft *xsql.ArrayType, srcSlice []interface{}) (interface{}, error) {
	if ft.FieldType != nil { //complex type array or struct
		switch st := ft.FieldType.(type) { //Only two complex types supported here
		case *xsql.ArrayType: //TODO handle array of array. Now the type is treated as interface{}
			var tempSlice [][]interface{}
			for i, t := range srcSlice{
				if reflect.ValueOf(t).Kind() == reflect.Array{
					if tempArr, err := addArrayField(st, t.([]interface{})); err !=nil{
						return nil, err
					}else {
						tempSlice = append(tempSlice, tempArr.([]interface{}))
					}
				}else{
					return nil, errors.New(fmt.Sprintf("invalid data type for [%d], expect array but found %s", i, t))
				}
			}
			return tempSlice, nil
		case *xsql.RecType:
			var tempSlice []map[string]interface{}
			for i, t := range srcSlice{
				if reflect.ValueOf(t).Kind() == reflect.Map{
					j, ok := t.(map[string]interface{})
					if !ok {
						return nil, errors.New(fmt.Sprintf("invalid data type for [%d], expect map but found %s", i, t))
					}
					r := make(map[string]interface{})
					for _, f := range st.StreamFields {
						p := f.Name
						if e := addRecField(f.FieldType, r, j, p); e != nil{
							return nil, e
						}
					}
					tempSlice = append(tempSlice, r)
				}else{
					return nil, errors.New(fmt.Sprintf("invalid data type for [%d], expect float but found %s", i, t))
				}
			}
			return tempSlice, nil
		default:
			return nil, errors.New(fmt.Sprintf("unsupported type %T", st))
		}
	}else{ //basic type
		switch ft.Type {
		case xsql.UNKNOWN:
			return nil, errors.New(fmt.Sprintf("invalid data type unknown defined for %s, please checke the stream definition", srcSlice))
		case xsql.BIGINT:
			var tempSlice []int
			for i, t := range srcSlice {
				if reflect.ValueOf(t).Kind() == reflect.Float64{
					tempSlice = append(tempSlice, int(t.(float64)))
				}else{
					return nil, errors.New(fmt.Sprintf("invalid data type for [%d], expect float but found %s", i, t))
				}
			}
			return tempSlice, nil
		case xsql.FLOAT:
			var tempSlice []float64
			for i, t := range srcSlice {
				if reflect.ValueOf(t).Kind() == reflect.Float64{
					tempSlice = append(tempSlice, t.(float64))
				}else{
					return nil, errors.New(fmt.Sprintf("invalid data type for [%d], expect float but found %s", i, t))
				}
			}
			return tempSlice, nil
		case xsql.STRINGS:
			var tempSlice []string
			for i, t := range srcSlice {
				if reflect.ValueOf(t).Kind() == reflect.String{
					tempSlice = append(tempSlice, t.(string))
				}else{
					return nil, errors.New(fmt.Sprintf("invalid data type for [%d], expect string but found %s", i, t))
				}
			}
			return tempSlice, nil
		case xsql.DATETIME:
			return nil, errors.New(fmt.Sprintf("invalid data type for %s, datetime type is not supported yet", srcSlice))
		case xsql.BOOLEAN:
			var tempSlice []bool
			for i, t := range srcSlice {
				if reflect.ValueOf(t).Kind() == reflect.Bool{
					tempSlice = append(tempSlice, t.(bool))
				}else{
					return nil, errors.New(fmt.Sprintf("invalid data type for [%d], expect boolean but found %s", i, t))
				}
			}
			return tempSlice, nil
		default:
			return nil, errors.New(fmt.Sprintf("invalid data type for %T, datetime type is not supported yet", ft.Type))
		}
	}
}