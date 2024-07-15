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

import (
	"time"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type RuleOption struct {
	Debug              bool              `json:"debug,omitempty" yaml:"debug,omitempty"`
	LogFilename        string            `json:"logFilename,omitempty" yaml:"logFilename,omitempty"`
	IsEventTime        bool              `json:"isEventTime,omitempty" yaml:"isEventTime,omitempty"`
	LateTol            cast.DurationConf `json:"lateTolerance,omitempty" yaml:"lateTolerance,omitempty"`
	Concurrency        int               `json:"concurrency" yaml:"concurrency"`
	BufferLength       int               `json:"bufferLength" yaml:"bufferLength"`
	SendMetaToSink     bool              `json:"sendMetaToSink,omitempty" yaml:"sendMetaToSink,omitempty"`
	SendError          bool              `json:"sendError,omitempty" yaml:"sendError,omitempty"`
	Qos                Qos               `json:"qos,omitempty" yaml:"qos,omitempty"`
	CheckpointInterval cast.DurationConf `json:"checkpointInterval,omitempty" yaml:"checkpointInterval,omitempty"`
	Restart            *RestartStrategy  `json:"restartStrategy,omitempty" yaml:"restartStrategy,omitempty"`
	Cron               string            `json:"cron,omitempty" yaml:"cron,omitempty"`
	Duration           string            `json:"duration,omitempty" yaml:"duration,omitempty"`
	CronDatetimeRange  []DatetimeRange   `json:"cronDatetimeRange,omitempty" yaml:"cronDatetimeRange,omitempty"`
}

type DatetimeRange struct {
	Begin          string `json:"begin" yaml:"begin"`
	End            string `json:"end" yaml:"end"`
	BeginTimestamp int64  `json:"beginTimestamp" yaml:"beginTimestamp"`
	EndTimestamp   int64  `json:"endTimestamp" yaml:"endTimestamp"`
}

type RestartStrategy struct {
	Attempts     int               `json:"attempts" yaml:"attempts"`
	Delay        cast.DurationConf `json:"delay" yaml:"delay"`
	Multiplier   float64           `json:"multiplier" yaml:"multiplier"`
	MaxDelay     cast.DurationConf `json:"maxDelay" yaml:"maxDelay"`
	JitterFactor float64           `json:"jitter" yaml:"jitter"`
}

type PrintableTopo struct {
	Sources []string                 `json:"sources" yaml:"sources"`
	Edges   map[string][]interface{} `json:"edges" yaml:"edges"`
}

type GraphNode struct {
	Type     string                 `json:"type" yaml:"type"`
	NodeType string                 `json:"nodeType" yaml:"nodeType"`
	Props    map[string]interface{} `json:"props" yaml:"props"`
	// UI is a placeholder for ui properties
	UI map[string]interface{} `json:"ui" yaml:"ui"`
}

// SourceMeta is the metadata of a source node. It describes what existed stream/table to refer to.
// It is part of the Props in the GraphNode and it is optional
type SourceMeta struct {
	SourceName string `json:"sourceName"` // the name of the stream or table
	SourceType string `json:"sourceType"` // stream or table
}

type RuleGraph struct {
	Nodes map[string]*GraphNode `json:"nodes" yaml:"nodes"`
	Topo  *PrintableTopo        `json:"topo" yaml:"topo"`
}

// Rule the definition of the business logic
// Sql and Graph are mutually exclusive, at least one of them should be set
type Rule struct {
	Triggered bool                     `json:"triggered" yaml:"triggered"`
	Id        string                   `json:"id,omitempty" yaml:"id,omitempty"`
	Name      string                   `json:"name,omitempty" yaml:"name,omitempty"` // The display name of a rule
	Sql       string                   `json:"sql,omitempty" yaml:"sql,omitempty"`
	Graph     *RuleGraph               `json:"graph,omitempty" yaml:"graph,omitempty"`
	Actions   []map[string]interface{} `json:"actions,omitempty" yaml:"actions,omitempty"`
	Options   *RuleOption              `json:"options,omitempty" yaml:"options,omitempty"`
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
			LateTol:            cast.DurationConf(time.Second),
			IsEventTime:        false,
			Concurrency:        1,
			BufferLength:       1024,
			SendMetaToSink:     false,
			SendError:          true,
			Qos:                AtMostOnce,
			CheckpointInterval: cast.DurationConf(5 * time.Minute),
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
