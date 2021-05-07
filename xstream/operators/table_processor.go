package operators

import (
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
)

type TableProcessor struct {
	//Pruned stream fields. Could be streamField(with data type info) or string
	defaultFieldProcessor

	isBatchInput bool // whether the inputs are batched, such as file which sends multiple messages at a batch. If batch input, only fires when EOF is received. This is mutual exclusive with retainSize.
	retainSize   int  // how many(maximum) messages to be retained for each output
	// States
	output       xsql.WindowTuples // current batched message collection
	batchEmitted bool              // if batch input, this is the signal for whether the last batch has emitted. If true, reinitialize.
}

func NewTableProcessor(name string, fields []interface{}, fs xsql.Fields, options *xsql.Options) (*TableProcessor, error) {
	p := &TableProcessor{
		output: xsql.WindowTuples{
			Emitter: name,
			Tuples:  make([]xsql.Tuple, 0),
		},
	}
	p.defaultFieldProcessor = defaultFieldProcessor{
		streamFields: fields, aliasFields: fs, isBinary: false, timestampFormat: options.TIMESTAMP_FORMAT,
	}
	if options.RETAIN_SIZE > 0 {
		p.retainSize = options.RETAIN_SIZE
		p.isBatchInput = false
	} else if isBatch(options.TYPE) {
		p.isBatchInput = true
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
		p.output.Tuples = make([]xsql.Tuple, 0)
		p.batchEmitted = false
	}
	if tuple.Message != nil {
		result, err := p.processField(tuple, fv)
		if err != nil {
			return fmt.Errorf("error in table processor: %s", err)
		}
		tuple.Message = result
		if p.retainSize > 0 && len(p.output.Tuples) == p.retainSize {
			p.output.Tuples = p.output.Tuples[1:]
		}
		p.output.Tuples = append(p.output.Tuples, *tuple)
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
