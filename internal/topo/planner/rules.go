package planner

type logicalOptRule interface {
	optimize(LogicalPlan) (LogicalPlan, error)
	name() string
}

type predicatePushDown struct{}

func (r *predicatePushDown) optimize(lp LogicalPlan) (LogicalPlan, error) {
	_, p := lp.PushDownPredicate(nil)
	return p, nil
}

func (r *predicatePushDown) name() string {
	return "predicatePushDown"
}

type columnPruner struct{}

func (r *columnPruner) optimize(lp LogicalPlan) (LogicalPlan, error) {
	err := lp.PruneColumns(nil)
	return lp, err
}

func (r *columnPruner) name() string {
	return "columnPruner"
}
