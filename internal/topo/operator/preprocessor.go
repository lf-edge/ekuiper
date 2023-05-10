// Copyright 2021-2022 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package operator

import (
	"fmt"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/lf-edge/ekuiper/pkg/message"
)

// Preprocessor only planned when
// 1. eventTime, to convert the timestamp field
// 2. schema validate and convert, when strict_validation is on and field type is not binary
// Do not convert types
type Preprocessor struct {
	//Pruned stream fields. Could be streamField(with data type info) or string
	defaultFieldProcessor
	//allMeta        bool
	//metaFields     []string //only needed if not allMeta
	isEventTime    bool
	timestampField string
	checkSchema    bool
	isBinary       bool
}

func NewPreprocessor(isSchemaless bool, fields map[string]*ast.JsonStreamField, _ bool, _ []string, iet bool, timestampField string, timestampFormat string, isBinary bool, strictValidation bool) (*Preprocessor, error) {
	p := &Preprocessor{
		isEventTime: iet, timestampField: timestampField, isBinary: isBinary}
	conf.Log.Infof("preprocessor isSchemaless %v, strictValidation %v, isBinary %v", isSchemaless, strictValidation, strictValidation)
	if !isSchemaless && (strictValidation || isBinary) {
		p.checkSchema = true
		conf.Log.Infof("preprocessor check schema")
		p.defaultFieldProcessor = defaultFieldProcessor{
			streamFields: fields, timestampFormat: timestampFormat,
		}
	}
	return p, nil
}

// Apply the preprocessor to the tuple
/*	input: *xsql.Tuple
 *	output: *xsql.Tuple
 */
func (p *Preprocessor) Apply(ctx api.StreamContext, data interface{}, _ *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	log := ctx.GetLogger()
	tuple, ok := data.(*xsql.Tuple)
	if !ok {
		return fmt.Errorf("expect tuple data type")
	}

	log.Debugf("preprocessor receive %s", tuple.Message)
	if p.checkSchema {
		if !p.isBinary {
			err := p.validateAndConvert(tuple)
			if err != nil {
				return fmt.Errorf("error in preprocessor: %s", err)
			}
		} else {
			for name := range p.streamFields {
				tuple.Message[name] = tuple.Message[message.DefaultField]
				delete(tuple.Message, message.DefaultField)
				break
			}
		}
	}
	if p.isEventTime {
		if t, ok := tuple.Message[p.timestampField]; ok {
			if ts, err := cast.InterfaceToUnixMilli(t, p.timestampFormat); err != nil {
				return fmt.Errorf("cannot convert timestamp field %s to timestamp with error %v", p.timestampField, err)
			} else {
				tuple.Timestamp = ts
				log.Debugf("preprocessor calculate timestamp %d", tuple.Timestamp)
			}
		} else {
			return fmt.Errorf("cannot find timestamp field %s in tuple %v", p.timestampField, tuple.Message)
		}
	}
	// No need to reconstruct meta as the memory has been allocated earlier
	//if !p.allMeta && p.metaFields != nil && len(p.metaFields) > 0 {
	//	newMeta := make(xsql.Metadata)
	//	for _, f := range p.metaFields {
	//		if m, ok := tuple.Metadata.Value(f, ""); ok {
	//			newMeta[f] = m
	//		}
	//	}
	//	tuple.Metadata = newMeta
	//}
	return tuple
}
