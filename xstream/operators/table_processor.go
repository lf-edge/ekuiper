package operators

import (
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
)

type TableProcessor struct {
	//Pruned stream fields. Could be streamField(with data type info) or string
	defaultFieldProcessor
}

func NewTableProcessor(fields []interface{}, fs xsql.Fields, timestampFormat string) (*TableProcessor, error) {
	p := &TableProcessor{}
	p.defaultFieldProcessor = defaultFieldProcessor{
		streamFields: fields, aliasFields: fs, isBinary: false, timestampFormat: timestampFormat,
	}
	return p, nil
}

/*
 *	input: []*xsql.Tuple
 *	output: WindowTuples
 */
func (p *TableProcessor) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	logger := ctx.GetLogger()
	tuples, ok := data.([]*xsql.Tuple)
	if !ok {
		return fmt.Errorf("expect []*xsql.Tuple data type")
	}
	logger.Debugf("Start to process table fields")
	w := xsql.WindowTuples{
		Emitter: tuples[0].Emitter,
		Tuples:  make([]xsql.Tuple, len(tuples)),
	}
	for i, t := range tuples {
		result, err := p.processField(t, fv)
		if err != nil {
			return fmt.Errorf("error in table processor: %s", err)
		}
		t.Message = result
		w.Tuples[i] = *t
	}
	return w
}
