package operator

import (
	"fmt"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

type OrderOp struct {
	SortFields ast.SortFields
}

/**
 *  input: *xsql.Tuple from preprocessor | xsql.WindowTuplesSet from windowOp | xsql.JoinTupleSets from joinOp
 *  output: *xsql.Tuple | xsql.WindowTuplesSet | xsql.JoinTupleSets
 */
func (p *OrderOp) Apply(ctx api.StreamContext, data interface{}, fv *xsql.FunctionValuer, _ *xsql.AggregateFunctionValuer) interface{} {
	log := ctx.GetLogger()
	log.Debugf("order plan receive %s", data)
	sorter := xsql.OrderedBy(p.SortFields, fv)
	switch input := data.(type) {
	case error:
		return input
	case xsql.Valuer:
		return input
	case xsql.SortingData:
		if err := sorter.Sort(input); err != nil {
			return fmt.Errorf("run Order By error: %s", err)
		}
		return input
	default:
		return fmt.Errorf("run Order By error: expect xsql.Valuer or its array type")
	}
}
