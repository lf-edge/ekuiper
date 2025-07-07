package planner

import (
	"github.com/lf-edge/ekuiper/v2/internal/v2/catalog"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

type LogicalPlan interface {
	GetIndex() int64
	GetChildren() []LogicalPlan
	AddChild(LogicalPlan)
	SetChildren([]LogicalPlan)
}

type BaseLogicalPlan struct {
	Index    int64
	Children []LogicalPlan
}

func (b *BaseLogicalPlan) SetChildren(plans []LogicalPlan) {
	//TODO implement me
	panic("implement me")
}

func (b *BaseLogicalPlan) AddChild(plan LogicalPlan) {
	//TODO implement me
	panic("implement me")
}

func (b *BaseLogicalPlan) GetIndex() int64 {
	//TODO implement me
	panic("implement me")
}

func (b *BaseLogicalPlan) GetChildren() []LogicalPlan {
	//TODO implement me
	panic("implement me")
}

func NewBaseLogicalPlan(Index int64) *BaseLogicalPlan {
	return &BaseLogicalPlan{Index: Index, Children: make([]LogicalPlan, 0)}
}

type LogicalPlanRoot struct {
	*BaseLogicalPlan
}

type LogicalPlanEnd struct {
	*BaseLogicalPlan
}

type DataSourcePlan struct {
	Stream *catalog.Stream
	*BaseLogicalPlan
}

func NewDataSourcePlan(stream *catalog.Stream, index int64) *DataSourcePlan {
	return &DataSourcePlan{Stream: stream, BaseLogicalPlan: NewBaseLogicalPlan(index)}
}

type ProjectPlan struct {
	Fields ast.Fields
	*BaseLogicalPlan
}

type PhysicalPlan interface {
	GetIndex() int64
	GetChildren() []PhysicalPlan
	AddChild(PhysicalPlan)
	SetChildren([]PhysicalPlan)
}

type BasePhysicalPlan struct {
	Index    int64
	Children []PhysicalPlan
}

func (b *BasePhysicalPlan) AddChild(plan PhysicalPlan) {
	//TODO implement me
	panic("implement me")
}

func (b *BasePhysicalPlan) SetChildren(plans []PhysicalPlan) {
	//TODO implement me
	panic("implement me")
}

func (b *BasePhysicalPlan) GetIndex() int64 {
	//TODO implement me
	panic("implement me")
}

func (b *BasePhysicalPlan) GetChildren() []PhysicalPlan {
	//TODO implement me
	panic("implement me")
}

type PhysicalDataSource struct {
	*BasePhysicalPlan
}

type PhysicalProject struct {
	*BasePhysicalPlan
}

type PhysicalStake struct {
	*BasePhysicalPlan
}
