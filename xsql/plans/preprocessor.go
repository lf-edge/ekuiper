package plans

import (
	"context"
	"engine/common"
	"engine/xsql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type Preprocessor struct {
	streamStmt  *xsql.StreamStmt
	isEventTime bool
	timestampField string
	timestampFormat string
}

func NewPreprocessor(s *xsql.StreamStmt, iet bool) (*Preprocessor, error){
	p := &Preprocessor{streamStmt: s, isEventTime: iet}
	if iet {
		if tf, ok := s.Options["TIMESTAMP"]; ok{
			p.timestampField = tf
		}else{
			return nil, fmt.Errorf("preprocessor is set to be event time but stream option TIMESTAMP not found")
		}
		if ts, ok := s.Options["TIMESTAMP_FORMAT"]; ok{
			p.timestampFormat = ts
		}
	}

	return p, nil
}

/*
 *	input: *xsql.Tuple
 *	output: *xsql.Tuple
 */
func (p *Preprocessor) Apply(ctx context.Context, data interface{}) interface{} {
	log := common.GetLogger(ctx)
	tuple, ok := data.(*xsql.Tuple)
	if !ok {
		log.Errorf("Expect tuple data type")
		return nil
	}

	log.Debugf("preprocessor receive %s", tuple.Message)

	result := make(map[string]interface{})
	for _, f := range p.streamStmt.StreamFields {
		fname := strings.ToLower(f.Name)
		if e := p.addRecField(f.FieldType, result, tuple.Message, fname); e != nil{
			log.Errorf("error in preprocessor: %s", e)
			return nil
		}
	}

	tuple.Message = result
	if p.isEventTime{
		if t, ok := result[p.timestampField]; ok{
			if ts, err := common.InterfaceToUnixMilli(t, p.timestampFormat); err != nil{
				log.Errorf("cannot convert timestamp field %s to timestamp with error %v", p.timestampField, err)
				return nil
			}else{
				tuple.Timestamp = ts
				log.Debugf("preprocessor calculate timstamp %d", tuple.Timestamp)
			}
		}else{
			log.Errorf("cannot find timestamp field %s in tuple %v", p.timestampField, result)
			return nil
		}
	}
	return tuple
}

func (p *Preprocessor) parseTime(s string) (time.Time, error){
	if f, ok := p.streamStmt.Options["TIMESTAMP_FORMAT"]; ok{
		return common.ParseTime(s, f)
	}else{
		return time.Parse(common.JSISO, s)
	}
}

func (p *Preprocessor) addRecField(ft xsql.FieldType, r map[string]interface{}, j map[string]interface{}, n string) error {
	if t, ok := j[n]; ok {
		v := reflect.ValueOf(t)
		jtype := v.Kind()
		switch st := ft.(type) {
		case *xsql.BasicType:
			switch st.Type {
			case xsql.UNKNOWN:
				return fmt.Errorf("invalid data type unknown defined for %s, please check the stream definition", t)
			case xsql.BIGINT:
				if jtype == reflect.Int{
					r[n] = t.(int)
				}else if jtype == reflect.Float64{
					r[n] = int(t.(float64))
				}else{
					return fmt.Errorf("invalid data type for %s, expect bigint but found %s", n, t)
				}
			case xsql.FLOAT:
				if jtype == reflect.Float64{
					r[n] = t.(float64)
				}else{
					return fmt.Errorf("invalid data type for %s, expect float but found %s", n, t)
				}
			case xsql.STRINGS:
				if jtype == reflect.String{
					r[n] = t.(string)
				}else{
					return fmt.Errorf("invalid data type for %s, expect string but found %s", n, t)
				}
			case xsql.DATETIME:
				switch jtype {
				case reflect.Int:
					ai := t.(int64)
					r[n] = common.TimeFromUnixMilli(ai)
				case reflect.Float64:
					ai := int64(t.(float64))
					r[n] = common.TimeFromUnixMilli(ai)
				case reflect.String:
					if t, err := p.parseTime(t.(string)); err != nil{
						return fmt.Errorf("invalid data type for %s, cannot convert to datetime: %s", n, err)
					}else{
						r[n] = t
					}
				default:
					return fmt.Errorf("invalid data type for %s, expect datatime but find %v", n, t)
				}
			case xsql.BOOLEAN:
				if jtype == reflect.Bool{
					r[n] = t.(bool)
				}else{
					return fmt.Errorf("invalid data type for %s, expect boolean but found %s", n, t)
				}
			default:
				return fmt.Errorf("invalid data type for %s, it is not supported yet", st)
			}
		case *xsql.ArrayType:
			if jtype != reflect.Slice{
				return fmt.Errorf("invalid data type for %s, expect array but found %s", n, t)
			}
			if tempArr, err := p.addArrayField(st, t.([]interface{})); err !=nil{
				return err
			}else {
				r[n] = tempArr
			}
		case *xsql.RecType:
			if jtype != reflect.Map{
				return fmt.Errorf("invalid data type for %s, expect struct but found %s", n, t)
			}
			nextJ, ok := j[n].(map[string]interface{})
			if !ok {
				return fmt.Errorf("invalid data type for %s, expect map but found %s", n, t)
			}
			nextR := make(map[string]interface{})
			for _, nextF := range st.StreamFields {
				nextP := strings.ToLower(nextF.Name)
				if e := p.addRecField(nextF.FieldType, nextR, nextJ, nextP); e != nil{
					return e
				}
			}
			r[n] = nextR
		default:
			return fmt.Errorf("unsupported type %T", st)
		}
		return nil
	}else{
		return fmt.Errorf("invalid data %s, field %s not found", j, n)
	}
}

//ft must be xsql.ArrayType
//side effect: r[p] will be set to the new array
func (p *Preprocessor) addArrayField(ft *xsql.ArrayType, srcSlice []interface{}) (interface{}, error) {
	if ft.FieldType != nil { //complex type array or struct
		switch st := ft.FieldType.(type) { //Only two complex types supported here
		case *xsql.ArrayType: //TODO handle array of array. Now the type is treated as interface{}
			var tempSlice [][]interface{}
			for i, t := range srcSlice{
				if reflect.ValueOf(t).Kind() == reflect.Array{
					if tempArr, err := p.addArrayField(st, t.([]interface{})); err !=nil{
						return nil, err
					}else {
						tempSlice = append(tempSlice, tempArr.([]interface{}))
					}
				}else{
					return nil, fmt.Errorf("invalid data type for [%d], expect array but found %s", i, t)
				}
			}
			return tempSlice, nil
		case *xsql.RecType:
			var tempSlice []map[string]interface{}
			for i, t := range srcSlice{
				if reflect.ValueOf(t).Kind() == reflect.Map{
					j, ok := t.(map[string]interface{})
					if !ok {
						return nil, fmt.Errorf("invalid data type for [%d], expect map but found %s", i, t)
					}
					r := make(map[string]interface{})
					for _, f := range st.StreamFields {
						n := f.Name
						if e := p.addRecField(f.FieldType, r, j, n); e != nil{
							return nil, e
						}
					}
					tempSlice = append(tempSlice, r)
				}else{
					return nil, fmt.Errorf("invalid data type for [%d], expect float but found %s", i, t)
				}
			}
			return tempSlice, nil
		default:
			return nil, fmt.Errorf("unsupported type %T", st)
		}
	}else{ //basic type
		switch ft.Type {
		case xsql.UNKNOWN:
			return nil, fmt.Errorf("invalid data type unknown defined for %s, please checke the stream definition", srcSlice)
		case xsql.BIGINT:
			var tempSlice []int
			for i, t := range srcSlice {
				if reflect.ValueOf(t).Kind() == reflect.Float64{
					tempSlice = append(tempSlice, int(t.(float64)))
				}else{
					return nil, fmt.Errorf("invalid data type for [%d], expect float but found %s", i, t)
				}
			}
			return tempSlice, nil
		case xsql.FLOAT:
			var tempSlice []float64
			for i, t := range srcSlice {
				if reflect.ValueOf(t).Kind() == reflect.Float64{
					tempSlice = append(tempSlice, t.(float64))
				}else{
					return nil, fmt.Errorf("invalid data type for [%d], expect float but found %s", i, t)
				}
			}
			return tempSlice, nil
		case xsql.STRINGS:
			var tempSlice []string
			for i, t := range srcSlice {
				if reflect.ValueOf(t).Kind() == reflect.String{
					tempSlice = append(tempSlice, t.(string))
				}else{
					return nil, fmt.Errorf("invalid data type for [%d], expect string but found %s", i, t)
				}
			}
			return tempSlice, nil
		case xsql.DATETIME:
			var tempSlice []time.Time
			for i, t := range srcSlice {
				jtype := reflect.ValueOf(t).Kind()
				switch jtype {
				case reflect.Int:
					ai := t.(int64)
					tempSlice = append(tempSlice, common.TimeFromUnixMilli(ai))
				case reflect.Float64:
					ai := int64(t.(float64))
					tempSlice = append(tempSlice, common.TimeFromUnixMilli(ai))
				case reflect.String:
					if ai, err := p.parseTime(t.(string)); err != nil{
						return nil, fmt.Errorf("invalid data type for %s, cannot convert to datetime: %s", t, err)
					}else{
						tempSlice = append(tempSlice, ai)
					}
				default:
					return nil, fmt.Errorf("invalid data type for [%d], expect datetime but found %v", i, t)
				}
			}
			return tempSlice, nil
		case xsql.BOOLEAN:
			var tempSlice []bool
			for i, t := range srcSlice {
				if reflect.ValueOf(t).Kind() == reflect.Bool{
					tempSlice = append(tempSlice, t.(bool))
				}else{
					return nil, fmt.Errorf("invalid data type for [%d], expect boolean but found %s", i, t)
				}
			}
			return tempSlice, nil
		default:
			return nil, fmt.Errorf("invalid data type for %T, datetime type is not supported yet", ft.Type)
		}
	}
}