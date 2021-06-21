package operators

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
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

func NewPreprocessor(fields []interface{}, allMeta bool, metaFields []string, iet bool, timestampField string, timestampFormat string, isBinary bool) (*Preprocessor, error) {
	p := &Preprocessor{
		allMeta: allMeta, metaFields: metaFields, isEventTime: iet, timestampField: timestampField}
	p.defaultFieldProcessor = defaultFieldProcessor{
		streamFields: fields, isBinary: isBinary, timestampFormat: timestampFormat,
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
