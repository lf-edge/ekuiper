package plans

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Preprocessor struct {
	streamStmt      *xsql.StreamStmt
	fields          xsql.Fields
	isEventTime     bool
	timestampField  string
	timestampFormat string
}

func NewPreprocessor(s *xsql.StreamStmt, fs xsql.Fields, iet bool) (*Preprocessor, error) {
	p := &Preprocessor{streamStmt: s, fields: fs, isEventTime: iet}
	if iet {
		if tf, ok := s.Options["TIMESTAMP"]; ok {
			p.timestampField = tf
		} else {
			return nil, fmt.Errorf("preprocessor is set to be event time but stream option TIMESTAMP not found")
		}
		if ts, ok := s.Options["TIMESTAMP_FORMAT"]; ok {
			p.timestampFormat = ts
		}
	}

	return p, nil
}

/*
 *	input: *xsql.Tuple
 *	output: *xsql.Tuple
 */
func (p *Preprocessor) Apply(ctx api.StreamContext, data interface{}) interface{} {
	log := ctx.GetLogger()
	tuple, ok := data.(*xsql.Tuple)
	if !ok {
		return fmt.Errorf("expect tuple data type")
	}

	log.Debugf("preprocessor receive %s", tuple.Message)

	result := make(map[string]interface{})
	if p.streamStmt.StreamFields != nil {
		for _, f := range p.streamStmt.StreamFields {
			fname := strings.ToLower(f.Name)
			if e := p.addRecField(f.FieldType, result, tuple.Message, fname); e != nil {
				return fmt.Errorf("error in preprocessor: %s", e)
			}
		}
	} else {
		result = tuple.Message
	}

	//If the field has alias name, then evaluate the alias field before transfer it to proceeding operators, and put it into result.
	//Otherwise, the GROUP BY, ORDER BY statement cannot get the value.
	for _, f := range p.fields {
		if f.AName != "" && (!xsql.HasAggFuncs(f.Expr)) {
			ve := &xsql.ValuerEval{Valuer: xsql.MultiValuer(tuple, &xsql.FunctionValuer{})}
			v := ve.Eval(f.Expr)
			if _, ok := v.(error); ok {
				return v
			} else {
				result[strings.ToLower(f.AName)] = v
			}
		}
	}

	tuple.Message = result
	if p.isEventTime {
		if t, ok := result[p.timestampField]; ok {
			if ts, err := common.InterfaceToUnixMilli(t, p.timestampFormat); err != nil {
				return fmt.Errorf("cannot convert timestamp field %s to timestamp with error %v", p.timestampField, err)
			} else {
				tuple.Timestamp = ts
				log.Debugf("preprocessor calculate timstamp %d", tuple.Timestamp)
			}
		} else {
			return fmt.Errorf("cannot find timestamp field %s in tuple %v", p.timestampField, result)
		}
	}
	return tuple
}

func (p *Preprocessor) parseTime(s string) (time.Time, error) {
	if f, ok := p.streamStmt.Options["TIMESTAMP_FORMAT"]; ok {
		return common.ParseTime(s, f)
	} else {
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
				if jtype == reflect.Int {
					r[n] = t.(int)
				} else if jtype == reflect.Float64 {
					r[n] = int(t.(float64))
				} else if jtype == reflect.String {
					if i, err := strconv.Atoi(t.(string)); err != nil {
						return fmt.Errorf("invalid data type for %s, expect bigint but found %[2]T(%[2]v)", n, t)
					} else {
						r[n] = i
					}
				} else {
					return fmt.Errorf("invalid data type for %s, expect bigint but found %[2]T(%[2]v)", n, t)
				}
			case xsql.FLOAT:
				if jtype == reflect.Float64 {
					r[n] = t.(float64)
				} else if jtype == reflect.String {
					if f, err := strconv.ParseFloat(t.(string), 64); err != nil {
						return fmt.Errorf("invalid data type for %s, expect float but found %[2]T(%[2]v)", n, t)
					} else {
						r[n] = f
					}
				} else {
					return fmt.Errorf("invalid data type for %s, expect float but found %[2]T(%[2]v)", n, t)
				}
			case xsql.STRINGS:
				if jtype == reflect.String {
					r[n] = t.(string)
				} else {
					return fmt.Errorf("invalid data type for %s, expect string but found %[2]T(%[2]v)", n, t)
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
					if t, err := p.parseTime(t.(string)); err != nil {
						return fmt.Errorf("invalid data type for %s, cannot convert to datetime: %s", n, err)
					} else {
						r[n] = t
					}
				default:
					return fmt.Errorf("invalid data type for %s, expect datatime but find %[2]T(%[2]v)", n, t)
				}
			case xsql.BOOLEAN:
				if jtype == reflect.Bool {
					r[n] = t.(bool)
				} else if jtype == reflect.String {
					if i, err := strconv.ParseBool(t.(string)); err != nil {
						return fmt.Errorf("invalid data type for %s, expect boolean but found %[2]T(%[2]v)", n, t)
					} else {
						r[n] = i
					}
				} else {
					return fmt.Errorf("invalid data type for %s, expect boolean but found %[2]T(%[2]v)", n, t)
				}
			default:
				return fmt.Errorf("invalid data type for %s, it is not supported yet", st)
			}
		case *xsql.ArrayType:
			var s []interface{}
			if jtype == reflect.Slice {
				s = t.([]interface{})
			} else if jtype == reflect.String {
				err := json.Unmarshal([]byte(t.(string)), &s)
				if err != nil {
					return fmt.Errorf("invalid data type for %s, expect array but found %[2]T(%[2]v)", n, t)
				}
			} else {
				return fmt.Errorf("invalid data type for %s, expect array but found %[2]T(%[2]v)", n, t)
			}

			if tempArr, err := p.addArrayField(st, s); err != nil {
				return err
			} else {
				r[n] = tempArr
			}
		case *xsql.RecType:
			nextJ := make(map[string]interface{})
			if jtype == reflect.Map {
				nextJ, ok = t.(map[string]interface{})
				if !ok {
					return fmt.Errorf("invalid data type for %s, expect map but found %[2]T(%[2]v)", n, t)
				}
			} else if jtype == reflect.String {
				err := json.Unmarshal([]byte(t.(string)), &nextJ)
				if err != nil {
					return fmt.Errorf("invalid data type for %s, expect map but found %[2]T(%[2]v)", n, t)
				}
			} else {
				return fmt.Errorf("invalid data type for %s, expect struct but found %[2]T(%[2]v)", n, t)
			}
			nextR := make(map[string]interface{})
			for _, nextF := range st.StreamFields {
				nextP := strings.ToLower(nextF.Name)
				if e := p.addRecField(nextF.FieldType, nextR, nextJ, nextP); e != nil {
					return e
				}
			}
			r[n] = nextR
		default:
			return fmt.Errorf("unsupported type %T", st)
		}
		return nil
	} else {
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
			var s []interface{}
			for i, t := range srcSlice {
				jtype := reflect.ValueOf(t).Kind()
				if jtype == reflect.Slice || jtype == reflect.Array {
					s = t.([]interface{})
				} else if jtype == reflect.String {
					err := json.Unmarshal([]byte(t.(string)), &s)
					if err != nil {
						return nil, fmt.Errorf("invalid data type for [%d], expect array but found %[2]T(%[2]v)", i, t)
					}
				} else {
					return nil, fmt.Errorf("invalid data type for [%d], expect array but found %[2]T(%[2]v)", i, t)
				}
				if tempArr, err := p.addArrayField(st, s); err != nil {
					return nil, err
				} else {
					tempSlice = append(tempSlice, tempArr.([]interface{}))
				}
			}
			return tempSlice, nil
		case *xsql.RecType:
			var tempSlice []map[string]interface{}
			for i, t := range srcSlice {
				jtype := reflect.ValueOf(t).Kind()
				j := make(map[string]interface{})
				var ok bool
				if jtype == reflect.Map {
					j, ok = t.(map[string]interface{})
					if !ok {
						return nil, fmt.Errorf("invalid data type for [%d], expect map but found %[2]T(%[2]v)", i, t)
					}

				} else if jtype == reflect.String {
					err := json.Unmarshal([]byte(t.(string)), &j)
					if err != nil {
						return nil, fmt.Errorf("invalid data type for [%d], expect map but found %[2]T(%[2]v)", i, t)
					}
				} else {
					return nil, fmt.Errorf("invalid data type for [%d], expect map but found %[2]T(%[2]v)", i, t)
				}
				r := make(map[string]interface{})
				for _, f := range st.StreamFields {
					n := f.Name
					if e := p.addRecField(f.FieldType, r, j, n); e != nil {
						return nil, e
					}
				}
				tempSlice = append(tempSlice, r)
			}
			return tempSlice, nil
		default:
			return nil, fmt.Errorf("unsupported type %T", st)
		}
	} else { //basic type
		switch ft.Type {
		case xsql.UNKNOWN:
			return nil, fmt.Errorf("invalid data type unknown defined for %s, please checke the stream definition", srcSlice)
		case xsql.BIGINT:
			var tempSlice []int
			for i, t := range srcSlice {
				jtype := reflect.ValueOf(t).Kind()
				if jtype == reflect.Float64 {
					tempSlice = append(tempSlice, int(t.(float64)))
				} else if jtype == reflect.String {
					if v, err := strconv.Atoi(t.(string)); err != nil {
						return nil, fmt.Errorf("invalid data type for [%d], expect float but found %[2]T(%[2]v)", i, t)
					} else {
						tempSlice = append(tempSlice, v)
					}
				} else {
					return nil, fmt.Errorf("invalid data type for [%d], expect float but found %[2]T(%[2]v)", i, t)
				}
			}
			return tempSlice, nil
		case xsql.FLOAT:
			var tempSlice []float64
			for i, t := range srcSlice {
				jtype := reflect.ValueOf(t).Kind()
				if jtype == reflect.Float64 {
					tempSlice = append(tempSlice, t.(float64))
				} else if jtype == reflect.String {
					if f, err := strconv.ParseFloat(t.(string), 64); err != nil {
						return nil, fmt.Errorf("invalid data type for [%d], expect float but found %[2]T(%[2]v)", i, t)
					} else {
						tempSlice = append(tempSlice, f)
					}
				} else {
					return nil, fmt.Errorf("invalid data type for [%d], expect float but found %[2]T(%[2]v)", i, t)
				}
			}
			return tempSlice, nil
		case xsql.STRINGS:
			var tempSlice []string
			for i, t := range srcSlice {
				if reflect.ValueOf(t).Kind() == reflect.String {
					tempSlice = append(tempSlice, t.(string))
				} else {
					return nil, fmt.Errorf("invalid data type for [%d], expect string but found %[2]T(%[2]v)", i, t)
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
					if ai, err := p.parseTime(t.(string)); err != nil {
						return nil, fmt.Errorf("invalid data type for %s, cannot convert to datetime: %[2]T(%[2]v)", t, err)
					} else {
						tempSlice = append(tempSlice, ai)
					}
				default:
					return nil, fmt.Errorf("invalid data type for [%d], expect datetime but found %[2]T(%[2]v)", i, t)
				}
			}
			return tempSlice, nil
		case xsql.BOOLEAN:
			var tempSlice []bool
			for i, t := range srcSlice {
				jtype := reflect.ValueOf(t).Kind()
				if jtype == reflect.Bool {
					tempSlice = append(tempSlice, t.(bool))
				} else if jtype == reflect.String {
					if v, err := strconv.ParseBool(t.(string)); err != nil {
						return nil, fmt.Errorf("invalid data type for [%d], expect boolean but found %[2]T(%[2]v)", i, t)
					} else {
						tempSlice = append(tempSlice, v)
					}
				} else {
					return nil, fmt.Errorf("invalid data type for [%d], expect boolean but found %[2]T(%[2]v)", i, t)
				}
			}
			return tempSlice, nil
		default:
			return nil, fmt.Errorf("invalid data type for %T", ft.Type)
		}
	}
}
