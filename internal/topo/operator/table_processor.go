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
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

type TableProcessor struct {
	//Pruned stream fields. Could be streamField(with data type info) or string
	defaultFieldProcessor

	isSchemaless bool
	isBatchInput bool // whether the inputs are batched, such as file which sends multiple messages at a batch. If batch input, only fires when EOF is received. This is mutual exclusive with retainSize.
	retainSize   int  // how many(maximum) messages to be retained for each output
	emitterName  string
	// States
	output       *xsql.WindowTuples // current batched message collection
	batchEmitted bool               // if batch input, this is the signal for whether the last batch has emitted. If true, reinitialize.
}

func NewTableProcessor(isSchemaless bool, name string, fields []interface{}, options *ast.Options) (*TableProcessor, error) {
	p := &TableProcessor{emitterName: name, batchEmitted: true, retainSize: 1, isSchemaless: isSchemaless}
	p.defaultFieldProcessor = defaultFieldProcessor{
		streamFields: fields, isBinary: false, timestampFormat: options.TIMESTAMP_FORMAT,
		strictValidation: options.STRICT_VALIDATION,
	}
	if options.RETAIN_SIZE > 0 {
		p.retainSize = options.RETAIN_SIZE
		p.isBatchInput = false
	} else if isBatch(options.TYPE) {
		p.isBatchInput = true
		p.retainSize = 0
	}
	return p, nil
}

/*
 *	input: *xsql.Tuple or BatchCount
 *	output: WindowTuples
 */
func (p *TableProcessor) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	logger := ctx.GetLogger()
	tuple, ok := data.(*xsql.Tuple)
	if !ok {
		return fmt.Errorf("expect *xsql.Tuple data type")
	}
	logger.Debugf("preprocessor receive %v", tuple)
	if p.batchEmitted {
		p.output = &xsql.WindowTuples{
			Content: make([]xsql.Row, 0),
		}
		p.batchEmitted = false
	}
	if tuple.Message != nil {
		if !p.isSchemaless && p.streamFields != nil {
			result, err := p.processField(tuple, nil)
			if err != nil {
				return fmt.Errorf("error in preprocessor: %s", err)
			}
			tuple.Message = result
		}
		var newTuples []xsql.Row
		_ = p.output.Range(func(i int, r xsql.Row) (bool, error) {
			if p.retainSize > 0 && p.output.Len() == p.retainSize && i == 0 {
				return true, nil
			}
			newTuples = append(newTuples, r)
			return true, nil
		})
		newTuples = append(newTuples, tuple)
		p.output = &xsql.WindowTuples{
			Content: newTuples,
		}
		if !p.isBatchInput {
			return p.output
		}
	} else if p.isBatchInput { // EOF
		p.batchEmitted = true
		return p.output
	}
	return nil
}

func isBatch(t string) bool {
	return t == "file" || t == ""
}
