package node

import (
	"encoding/json"

	"github.com/lf-edge/ekuiper/v2/internal/v2/api"
)

type NodeMessage struct {
	Kind    string
	Control *NodeControlMessage
	Tuples  []*api.Tuple
	Err     error
}

func (nm *NodeMessage) IsSameControlSignal(c ControlSignal) bool {
	if nm.Control == nil {
		return false
	}
	return nm.Control.ControlSignal == c
}

type ControlSignal int

const (
	StartRuleSignal ControlSignal = iota
	StopRuleSignal
)

type NodeControlMessage struct {
	ControlSignal ControlSignal
}

func (nm *NodeMessage) TupleString() string {
	m := make([]map[string]any, 0)
	for _, tuple := range nm.Tuples {
		m = append(m, tuple.ToMap())
	}
	v, _ := json.Marshal(m)
	return string(v)
}

func NewSignalMsg(signal ControlSignal) *NodeMessage {
	return &NodeMessage{
		Control: &NodeControlMessage{
			ControlSignal: signal,
		},
	}
}

func NewStopRuleMsg() *NodeMessage {
	return &NodeMessage{
		Control: &NodeControlMessage{
			ControlSignal: StopRuleSignal,
		},
	}
}
