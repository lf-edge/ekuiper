// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"strings"
)

type Preprocessor struct {
	//Pruned stream fields. Could be streamField(with data type info) or string
	defaultFieldProcessor
	allMeta        bool
	metaFields     []string //only needed if not allMeta
	isEventTime    bool
	timestampField string
}

func NewPreprocessor(fields []interface{}, allMeta bool, metaFields []string, iet bool, timestampField string, timestampFormat string, isBinary bool, strictValidation bool) (*Preprocessor, error) {
	p := &Preprocessor{
		allMeta: allMeta, metaFields: metaFields, isEventTime: iet, timestampField: timestampField}
	p.defaultFieldProcessor = defaultFieldProcessor{
		streamFields: fields, isBinary: isBinary, timestampFormat: timestampFormat, strictValidation: strictValidation,
	}
	return p, nil
}

/*
 *	input: *xsql.Tuple
 *	output: *xsql.Tuple
 */
func (p *Preprocessor) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	log := ctx.GetLogger()
	tuple, ok := data.(*xsql.Tuple)
	if !ok {
		return fmt.Errorf("expect tuple data type")
	}

	log.Debugf("preprocessor receive %s", tuple.Message)

	result, err := p.processField(tuple, fv)
	if err != nil {
		return fmt.Errorf("error in preprocessor: %s", err)
	}

	tuple.Message = result
	if p.isEventTime {
		if t, ok := result[p.timestampField]; ok {
			if ts, err := cast.InterfaceToUnixMilli(t, p.timestampFormat); err != nil {
				return fmt.Errorf("cannot convert timestamp field %s to timestamp with error %v", p.timestampField, err)
			} else {
				tuple.Timestamp = ts
				log.Debugf("preprocessor calculate timstamp %d", tuple.Timestamp)
			}
		} else {
			return fmt.Errorf("cannot find timestamp field %s in tuple %v", p.timestampField, result)
		}
	}
	if !p.allMeta && p.metaFields != nil && len(p.metaFields) > 0 {
		newMeta := make(xsql.Metadata)
		for _, f := range p.metaFields {
			if m, ok := tuple.Metadata.Value(f); ok {
				newMeta[strings.ToLower(f)] = m
			}
		}
		tuple.Metadata = newMeta
	}
	return tuple
}
