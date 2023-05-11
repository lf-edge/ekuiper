// Copyright 2022-2023 EMQ Technologies Co., Ltd.
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

package planner

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/internal/pkg/store"
	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/ast"
)

func TestPlannerGraphValidate(t *testing.T) {
	tests := []struct {
		graph string
		err   string
	}{
		{
			graph: `{
  "nodes": {
    "abc": {
      "type": "source",
      "nodeType": "mqtt",
      "props": {
        "datasource": "demo"
      }
    },
    "myfilter": {
      "type": "operator",
      "nodeType": "filter",
      "props": {
        "expr": "temperature > 20"
      }
    },
    "logfunc": {
      "type": "operator",
      "nodeType": "function",
      "props": {
        "expr": "log(temperature) as log_temperature"
      }
    },
    "sinfunc": {
      "type": "operator",
      "nodeType": "function",
      "props": {
        "expr": "sin(temperature) as sin_temperature"
      }
    },
    "pick": {
      "type": "operator",
      "nodeType": "pick",
      "props": {
        "fields": [
          "log_temperature",
          "humidity"
        ]
      }
    },
    "mqttpv": {
      "type": "sink",
      "nodeType": "mqtt",
      "props": {
        "server": "tcp://syno.home:1883",
        "topic": "result",
        "sendSingle": true
      }
    },
    "mqtt2": {
      "type": "sink",
      "nodeType": "mqtt",
      "props": {
        "server": "tcp://syno.home:1883",
        "topic": "result2",
        "sendSingle": true
      }
    }
  },
  "topo": {
    "sources": [
      "abc"
    ],
    "edges": {
      "abc": [
        "myfilter",
        "sinfunc"
      ],
      "myfilter": [
        "logfunc"
      ],
      "logfunc": [
        "pick"
      ],
      "pick": [
        "mqttpv"
      ],
      "sinfunc": [
        "mqtt2"
      ]
    }
  }
}`,
			err: "",
		}, {
			graph: `{
  "nodes": {
    "abc": {
      "type": "source",
      "nodeType": "mqtt",
      "props": {
        "datasource": "demo"
      }
    },
    "mqtt2": {
      "type": "sink",
      "nodeType": "mqtt",
      "props": {
        "server": "tcp://syno.home:1883",
        "topic": "result2",
        "sendSingle": true
      }
    }
  },
  "topo": {
    "sources": [
      "abc"
    ],
    "edges": {
      "abc": [
        "myfilter"
      ]
    }
  }
}`,
			err: "node myfilter is not defined",
		}, {
			graph: `{
  "nodes": {
    "abc": {
      "type": "source",
      "nodeType": "mqtt",
      "props": {
        "datasource": "demo"
      }
    },
    "mqtt2": {
      "type": "sink",
      "nodeType": "mqtt",
      "props": {
        "server": "tcp://syno.home:1883",
        "topic": "result2",
        "sendSingle": true
      }
    }
  },
  "topo": {
    "sources": [
      "abc"
    ],
    "edges": {
    }
  }
}`,
			err: "no edge defined for source node abc",
		}, {
			graph: `{
  "nodes": {
    "abc": {
      "type": "source",
      "nodeType": "mqtt",
      "props": {
        "datasource": "demo"
      }
    },
    "aggfunc": {
      "type": "operator",
      "nodeType": "aggfunc",
      "props": {
        "expr": "avg(temperature) as avg_temperature"
      }
    },
    "mqtt2": {
      "type": "sink",
      "nodeType": "mqtt",
      "props": {
        "server": "tcp://syno.home:1883",
        "topic": "result2",
        "sendSingle": true
      }
    }
  },
  "topo": {
    "sources": [
      "abc"
    ],
    "edges": {
      "abc": ["aggfunc"],
      "aggfunc": ["mqtt2"]
    }
  }
}`,
			err: "node abc output does not match node aggfunc input: input type mismatch, expect collection, got row",
		}, {
			graph: `{
  "nodes": {
    "abc": {
      "type": "source",
      "nodeType": "mqtt",
      "props": {
        "datasource": "demo"
      }
    },
    "abc2": {
      "type": "source",
      "nodeType": "mqtt",
      "props": {
        "datasource": "demo1"
      }
    },
    "joinop": {
      "type": "operator",
      "nodeType": "join",
      "props": {
        "from": "abc",
        "joins": [
          {
            "name": "abc2",
            "type": "inner",
            "on": "abc.id = abc2.id"
          }
        ]
      }
    },
    "mqtt2": {
      "type": "sink",
      "nodeType": "mqtt",
      "props": {
        "server": "tcp://syno.home:1883",
        "topic": "result2",
        "sendSingle": true
      }
    }
  },
  "topo": {
    "sources": [
      "abc","abc2"
    ],
    "edges": {
      "abc": ["joinop"],
      "abc2": ["joinop"],
      "joinop": ["mqtt2"]
    }
  }
}`,
			err: "operator joinop of type join does not allow multiple inputs",
		}, {
			graph: `{
  "nodes": {
    "abc": {
      "type": "source",
      "nodeType": "mqtt",
      "props": {
        "datasource": "demo"
      }
    },
    "abc2": {
      "type": "source",
      "nodeType": "mqtt",
      "props": {
        "datasource": "demo1"
      }
    },
    "windowop": {
      "type": "operator",
      "nodeType": "window",
      "props": {
        "type": "hoppingwindow",
        "unit": "ss",
        "size": 10,
        "interval": 5
      }
    },
    "joinop": {
      "type": "operator",
      "nodeType": "join",
      "props": {
        "from": "abc",
        "joins": [
          {
            "name": "abc2",
            "type": "inner",
            "on": "abc.id = abc2.id"
          }
        ]
      }
    },
    "groupop": {
      "type": "operator",
      "nodeType": "groupby",
      "props": {
        "dimensions": ["id","userId"]
      }
    },
    "mqtt2": {
      "type": "sink",
      "nodeType": "mqtt",
      "props": {
        "server": "tcp://syno.home:1883",
        "topic": "result2",
        "sendSingle": true
      }
    }
  },
  "topo": {
    "sources": [
      "abc","abc2"
    ],
    "edges": {
      "abc": ["windowop"],
      "abc2": ["windowop"],
      "windowop": ["joinop"],
      "joinop": ["groupop"],
      "groupop": ["mqtt2"]
    }
  }
}`,
			err: "",
		}, {
			graph: `{
  "nodes": {
    "abc": {
      "type": "source",
      "nodeType": "mqtt",
      "props": {
        "datasource": "demo"
      }
    },
    "abc2": {
      "type": "source",
      "nodeType": "mqtt",
      "props": {
        "datasource": "demo1"
      }
    },
    "windowop": {
      "type": "operator",
      "nodeType": "window",
      "props": {
        "type": "hoppingwindow",
        "unit": "ss",
        "size": 10,
        "interval": 5
      }
    },
    "joinop": {
      "type": "operator",
      "nodeType": "join",
      "props": {
        "from": "abc",
        "joins": [
          {
            "name": "abc2",
            "type": "inner",
            "on": "abc.id = abc2.id"
          }
        ]
      }
    },
    "groupop": {
      "type": "operator",
      "nodeType": "groupby",
      "props": {
        "dimensions": ["id","userId"]
      }
    },
    "mqtt2": {
      "type": "sink",
      "nodeType": "mqtt",
      "props": {
        "server": "tcp://syno.home:1883",
        "topic": "result2",
        "sendSingle": true
      }
    }
  },
  "topo": {
    "sources": [
      "abc","abc2"
    ],
    "edges": {
      "abc": ["windowop"],
      "abc2": ["windowop"],
      "windowop": ["groupop"],
      "joinop": ["mqtt2"],
      "groupop": ["joinop"]
    }
  }
}`,
			err: "node groupop output does not match node joinop input: collection type mismatch, expect non-grouped collection, got grouped collection",
		}, {
			graph: `{
  "nodes": {
    "abc": {
      "type": "source",
      "nodeType": "mqtt",
      "props": {
        "datasource": "demo"
      }
    },
    "abc2": {
      "type": "source",
      "nodeType": "mqtt",
      "props": {
        "datasource": "demo1"
      }
    },
    "windowop": {
      "type": "operator",
      "nodeType": "window",
      "props": {
        "type": "hoppingwindow",
        "unit": "ss",
        "size": 10,
        "interval": 5
      }
    },
    "joinop": {
      "type": "operator",
      "nodeType": "join",
      "props": {
        "from": "abc",
        "joins": [
          {
            "name": "abc2",
            "type": "inner",
            "on": "abc.id = abc2.id"
          }
        ]
      }
    },
    "groupop": {
      "type": "operator",
      "nodeType": "groupby",
      "props": {
        "dimensions": ["id","userId"]
      }
    },
    "aggfunc": {
      "type": "operator",
      "nodeType": "aggFunc",
      "props": {
        "expr": "avg(temperature) as avg_temperature"
      }
    },
    "mqtt2": {
      "type": "sink",
      "nodeType": "mqtt",
      "props": {
        "server": "tcp://syno.home:1883",
        "topic": "result2",
        "sendSingle": true
      }
    }
  },
  "topo": {
    "sources": [
      "abc","abc2"
    ],
    "edges": {
      "abc": ["windowop"],
      "abc2": ["windowop"],
      "windowop": ["groupop"],
      "joinop": ["mqtt2"],
      "groupop": ["aggfunc"],
      "aggfunc": ["joinop"]
    }
  }
}`,
			err: "node aggfunc output does not match node joinop input: collection type mismatch, expect non-grouped collection, got grouped collection",
		},
	}

	t.Logf("The test bucket size is %d.\n\n", len(tests))
	for i, tt := range tests {
		rg := &api.RuleGraph{}
		err := json.Unmarshal([]byte(tt.graph), rg)
		if err != nil {
			t.Error(err)
			continue
		}
		_, err = PlanByGraph(&api.Rule{
			Triggered: false,
			Id:        fmt.Sprintf("rule%d", i),
			Name:      fmt.Sprintf("rule%d", i),
			Graph:     rg,
			Options: &api.RuleOption{
				IsEventTime:        false,
				LateTol:            1000,
				Concurrency:        1,
				BufferLength:       1024,
				SendMetaToSink:     false,
				SendError:          true,
				Qos:                api.AtMostOnce,
				CheckpointInterval: 300000,
			},
		})
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) {
			t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.err, err)
		}
	}
}

func TestPlannerGraphWithStream(t *testing.T) {
	store, err := store.GetKV("stream")
	if err != nil {
		t.Error(err)
		return
	}
	streamSqls := map[string]string{
		"src1": `CREATE STREAM src1 (
					id1 BIGINT,
					temp BIGINT,
					name string,
					myarray array(string)
				) WITH (DATASOURCE="src1", FORMAT="json", KEY="ts");`,
		"src2": `CREATE STREAM src2 (
					id2 BIGINT,
					hum BIGINT
				) WITH (DATASOURCE="src2", FORMAT="json", KEY="ts", TIMESTAMP_FORMAT="YYYY-MM-dd HH:mm:ss");`,
		"tableInPlanner": `CREATE TABLE tableInPlanner (
					id BIGINT,
					name STRING,
					value STRING,
					hum BIGINT
				) WITH (TYPE="file");`,
	}
	types := map[string]ast.StreamType{
		"src1":           ast.TypeStream,
		"src2":           ast.TypeStream,
		"tableInPlanner": ast.TypeTable,
	}
	for name, sql := range streamSqls {
		s, err := json.Marshal(&xsql.StreamInfo{
			StreamType: types[name],
			Statement:  sql,
		})
		if err != nil {
			t.Error(err)
			t.Fail()
		}
		err = store.Set(name, string(s))
		if err != nil {
			t.Error(err)
			t.Fail()
		}
	}
	testCases := []struct {
		name  string
		graph string
		err   error
	}{
		{
			name: "test stream",
			graph: `{
    "nodes": {
      "demo": {
        "type": "source",
        "nodeType": "mqtt",
        "props": {
          "sourceType": "stream",
          "sourceName": "src1"
        }
      },
      "log": {
        "type": "sink",
        "nodeType": "log",
        "props": {}
      }
    },
    "topo": {
      "sources": ["demo"],
      "edges": {
        "demo": ["log"]
      }
    }
}`,
			err: nil,
		},
		{
			name: "stream type wrong",
			graph: `{
    "nodes": {
      "demo": {
        "type": "source",
        "nodeType": "file",
        "props": {
          "sourceType": "stream",
          "sourceName": "src1"
        }
      },
      "log": {
        "type": "sink",
        "nodeType": "log",
        "props": {}
      }
    },
    "topo": {
      "sources": ["demo"],
      "edges": {
        "demo": ["log"]
      }
    }
}`,
			err: fmt.Errorf("source type file does not match the stream type mqtt"),
		},
		{
			name: "non exist stream",
			graph: `{
    "nodes": {
      "demo": {
        "type": "source",
        "nodeType": "mqtt",
        "props": {
          "sourceType": "stream",
          "sourceName": "unknown"
        }
      },
      "log": {
        "type": "sink",
        "nodeType": "log",
        "props": {}
      }
    },
    "topo": {
      "sources": ["demo"],
      "edges": {
        "demo": ["log"]
      }
    }
}`,
			err: fmt.Errorf("fail to get stream unknown, please check if stream is created"),
		},
		{
			name: "wrong source type",
			graph: `{
    "nodes": {
      "demo": {
        "type": "source",
        "nodeType": "mqtt",
        "props": {
          "sourceType": "stream",
          "sourceName": "tableInPlanner"
        }
      },
      "log": {
        "type": "sink",
        "nodeType": "log",
        "props": {}
      }
    },
    "topo": {
      "sources": ["demo"],
      "edges": {
        "demo": ["log"]
      }
    }
}`,
			err: fmt.Errorf("table tableInPlanner is not a stream"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rg := &api.RuleGraph{}
			err := json.Unmarshal([]byte(tc.graph), rg)
			if err != nil {
				t.Error(err)
				return
			}
			_, err = PlanByGraph(&api.Rule{
				Triggered: false,
				Id:        "test",
				Graph:     rg,
				Options: &api.RuleOption{
					IsEventTime:        false,
					LateTol:            1000,
					Concurrency:        1,
					BufferLength:       1024,
					SendMetaToSink:     false,
					SendError:          true,
					Qos:                api.AtMostOnce,
					CheckpointInterval: 300000,
				},
			})
			if tc.err == nil {
				if err != nil {
					t.Errorf("error mismatch:\n  exp=%s\n  got=%s\n\n", tc.err, err)
				}
				return
			}
			if !reflect.DeepEqual(tc.err.Error(), err.Error()) {
				t.Errorf("error mismatch:\n  exp=%s\n  got=%s\n\n", tc.err, err)
			}
		})
	}
}
