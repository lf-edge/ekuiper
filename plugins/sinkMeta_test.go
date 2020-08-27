package plugins

import (
	"github.com/emqx/kuiper/xstream/api"
	"testing"
)

func TestHintWhenModifySink(t *testing.T) {
	taosMeta := &uiSink{
		Fields: []*field{
			{
				Name:    "ip",
				Default: "911.911.911.911",
			},
		},
	}
	opMeta := &uiSink{
		Fields: []*field{
			{
				Name:    "isEventTime",
				Default: false,
			},
		},
	},
	baseMeta := &uiSink{
		Fields: []*field{
			{
				Name:    "bufferLength",
				Default: 911,
			},
		},
	},

	g_sinkMetadata = make(map[string]*uiSink)
	g_sinkMetadata["taos.json"] = taosMeta
	g_sinkMetadata["properties.json"] = baseMeta
	g_sinkMetadata["options.json"] = opMeta

	newSink := &uiSinks{
		CustomProperty: map[string]*uiSink{
			"taos": &uiSink{
				Fields: []*field{
					{
						Name:    "ip",
						Default: "114.114.114.114",
					},
				},
			},
		},
		BaseProperty: map[string]*uiSink{
			"taos": &uiSink{
				Fields: []*field{
					{
						Name:    "bufferLength",
						Default: 1024,
					},
				},
			},
		},

		BaseOption: &uiSink{
			Fields: []*field{
				{
					Name:    "isEventTime",
					Default: true,
				},
			},
		},
	}

	rule := &api.Rule{
		Actions: []map[string]interface{}{
			{
				"taos": map[string]interface{}{
					"ip":           "114.114.114.114",
					"bufferLength": 1024,
				},
			},
		},
		Options: &api.RuleOption{
			IsEventTime: true,
		},
	}

	oldSink := new(uiSinks)
	err := oldSink.hintWhenNewSink("taos")
	if nil != err {
		t.Error(err)
	}

	if false != oldSink.BaseOption.Fields[0].Default {
		t.Errorf("fail")
	}
	if 911 != oldSink.BaseProperty["taos"].Fields[0].Default {
		t.Errorf("fail")
	}
	if "911.911.911.911" != oldSink.CustomProperty["taos"].Fields[0].Default {
		t.Errorf("fail")
	}
	err = oldSink.hintWhenModifySink(rule)
	if nil != err {
		t.Error(err)
	}

	if oldSink.BaseOption.Fields[0].Default != newSink.BaseOption.Fields[0].Default {
		t.Errorf("fail")
	}
	if oldSink.BaseProperty["taos"].Fields[0].Default != newSink.BaseProperty["taos"].Fields[0].Default {
		t.Errorf("fail")
	}
	if oldSink.CustomProperty["taos"].Fields[0].Default != newSink.CustomProperty["taos"].Fields[0].Default {
		t.Errorf("fail")
	}
}
