// Copyright 2024 EMQ Technologies Co., Ltd.
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

package def

type RuleOption struct {
	Debug              bool             `json:"debug" yaml:"debug"`
	LogFilename        string           `json:"logFilename" yaml:"logFilename"`
	IsEventTime        bool             `json:"isEventTime" yaml:"isEventTime"`
	LateTol            int64            `json:"lateTolerance" yaml:"lateTolerance"`
	Concurrency        int              `json:"concurrency" yaml:"concurrency"`
	BufferLength       int              `json:"bufferLength" yaml:"bufferLength"`
	SendMetaToSink     bool             `json:"sendMetaToSink" yaml:"sendMetaToSink"`
	SendError          bool             `json:"sendError" yaml:"sendError"`
	Qos                Qos              `json:"qos" yaml:"qos"`
	CheckpointInterval any              `json:"checkpointInterval" yaml:"checkpointInterval"`
	Restart            *RestartStrategy `json:"restartStrategy" yaml:"restartStrategy"`
	Cron               string           `json:"cron" yaml:"cron"`
	Duration           string           `json:"duration" yaml:"duration"`
	CronDatetimeRange  []DatetimeRange  `json:"cronDatetimeRange" yaml:"cronDatetimeRange"`
}

type DatetimeRange struct {
	Begin          string `json:"begin" yaml:"begin"`
	End            string `json:"end" yaml:"end"`
	BeginTimestamp int64  `json:"beginTimestamp"`
	EndTimestamp   int64  `json:"endTimestamp"`
}

type RestartStrategy struct {
	Attempts     int     `json:"attempts" yaml:"attempts"`
	Delay        int     `json:"delay" yaml:"delay"`
	Multiplier   float64 `json:"multiplier" yaml:"multiplier"`
	MaxDelay     int     `json:"maxDelay" yaml:"maxDelay"`
	JitterFactor float64 `json:"jitter" yaml:"jitter"`
}

type PrintableTopo struct {
	Sources []string                 `json:"sources"`
	Edges   map[string][]interface{} `json:"edges"`
}

type GraphNode struct {
	Type     string                 `json:"type"`
	NodeType string                 `json:"nodeType"`
	Props    map[string]interface{} `json:"props"`
	// UI is a placeholder for ui properties
	UI map[string]interface{} `json:"ui"`
}

// SourceMeta is the metadata of a source node. It describes what existed stream/table to refer to.
// It is part of the Props in the GraphNode and it is optional
type SourceMeta struct {
	SourceName string `json:"sourceName"` // the name of the stream or table
	SourceType string `json:"sourceType"` // stream or table
}

type RuleGraph struct {
	Nodes map[string]*GraphNode `json:"nodes"`
	Topo  *PrintableTopo        `json:"topo"`
}

// Rule the definition of the business logic
// Sql and Graph are mutually exclusive, at least one of them should be set
type Rule struct {
	Triggered bool                     `json:"triggered"`
	Id        string                   `json:"id,omitempty"`
	Name      string                   `json:"name,omitempty"` // The display name of a rule
	Sql       string                   `json:"sql,omitempty"`
	Graph     *RuleGraph               `json:"graph,omitempty"`
	Actions   []map[string]interface{} `json:"actions,omitempty"`
	Options   *RuleOption              `json:"options,omitempty"`
}

func (r *Rule) IsLongRunningScheduleRule() bool {
	if r.Options == nil {
		return false
	}
	return len(r.Options.Cron) == 0 && len(r.Options.Duration) == 0 && len(r.Options.CronDatetimeRange) > 0
}

func (r *Rule) IsScheduleRule() bool {
	if r.Options == nil {
		return false
	}
	return len(r.Options.Cron) > 0 && len(r.Options.Duration) > 0
}

func GetDefaultRule(name, sql string) *Rule {
	return &Rule{
		Id:  name,
		Sql: sql,
		Options: &RuleOption{
			IsEventTime:        false,
			LateTol:            1000,
			Concurrency:        1,
			BufferLength:       1024,
			SendMetaToSink:     false,
			SendError:          true,
			Qos:                AtMostOnce,
			CheckpointInterval: "300s",
			Restart: &RestartStrategy{
				Attempts:     0,
				Delay:        1000,
				Multiplier:   2,
				MaxDelay:     30000,
				JitterFactor: 0.1,
			},
		},
	}
}

const (
	AtMostOnce Qos = iota
	AtLeastOnce
	ExactlyOnce
)

type Qos int
