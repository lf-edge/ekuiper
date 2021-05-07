package operators

import (
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
)

type TableProcessor struct {
	//Pruned stream fields. Could be streamField(with data type info) or string
	defaultFieldProcessor

	isBatchInput bool
	output       xsql.WindowTuples
	count        int
}

func NewTableProcessor(fields []interface{}, fs xsql.Fields, timestampFormat string, isBatchInput bool) (*TableProcessor, error) {
	p := &TableProcessor{isBatchInput: isBatchInput}
	p.defaultFieldProcessor = defaultFieldProcessor{
		streamFields: fields, aliasFields: fs, isBinary: false, timestampFormat: timestampFormat,
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

	if p.count == 0 {
		p.output = xsql.WindowTuples{
			Emitter: tuple.Emitter,
			Tuples:  make([]xsql.Tuple, 0),
		}
	}

	if tuple.Message != nil {
		result, err := p.processField(tuple, fv)
		if err != nil {
			return fmt.Errorf("error in table processor: %s", err)
		}
		tuple.Message = result
		p.output.Tuples = append(p.output.Tuples, *tuple)
		if !p.isBatchInput {
			return p.output
		} else {
			p.count = p.count + 1
		}
	} else if p.isBatchInput { // EOF
		p.count = 0
		return p.output
	}
	return nil
}
