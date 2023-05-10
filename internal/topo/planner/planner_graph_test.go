// Copyright 2022 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/internal/testx"
	"github.com/lf-edge/ekuiper/pkg/api"
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

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
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
