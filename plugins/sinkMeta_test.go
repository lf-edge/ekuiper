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

	g_sinkMetadata = make(map[string]*uiSink)
	g_sinkMetadata["taos.json"] = taosMeta
	g_sinkMetadata["log.json"] = logMeta

	oldSink := new(uiSinks)
	err := oldSink.hintWhenNewSink("taos")
	if nil != err {
		t.Error(err)
	}

	if "911.911.911.911" != oldSink.CustomProperty["taos"].Fields[0].Default {
		t.Errorf("fail")
	}
}
