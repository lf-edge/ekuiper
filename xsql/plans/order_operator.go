package plans

import (
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
)

type OrderPlan struct {
	SortFields xsql.SortFields
}

/**
  *  input: *xsql.Tuple from preprocessor | xsql.WindowTuplesSet from windowOp | xsql.JoinTupleSets from joinOp
  *  output: *xsql.Tuple | xsql.WindowTuplesSet | xsql.JoinTupleSets
 */
func (p *OrderPlan) Apply(ctx api.StreamContext, data interface{}) interface{} {
	log := ctx.GetLogger()
	log.Debugf("order plan receive %s", data)
	sorter := xsql.OrderedBy(p.SortFields)
	switch input := data.(type) {
	case xsql.Valuer:
		return input
	case xsql.SortingData:
		sorter.Sort(input)
		return input
	default:
		log.Errorf("Expect xsql.Valuer or its array type.")
		return nil
	}
}