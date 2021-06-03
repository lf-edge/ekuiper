package plugins

import (
	"testing"
)

func TestHintWhenModifySink(t *testing.T) {
	taosMeta := &uiSink{
		Fields: []field{
			{
				Name:    "ip",
				Default: "911.911.911.911",
			},
		},
	}
	logMeta := &uiSink{
		Fields: []field{
			{
				Name:    "ip",
				Default: "911.911.911.911",
			},
		},
	}
	opMeta := &uiSink{
		Fields: []field{
			{
				Name:    "isEventTime",
				Default: false,
			},
		},
	}
	baseMeta := &uiSink{
		Fields: []field{
			{
				Name:    "bufferLength",
				Default: 911,
			},
		},
	}

	g_sinkMetadata = make(map[string]*uiSink)
	g_sinkMetadata["taos.json"] = taosMeta
	g_sinkMetadata["log.json"] = logMeta
	g_sinkMetadata["properties.json"] = baseMeta
	g_sinkMetadata["options.json"] = opMeta

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
}
