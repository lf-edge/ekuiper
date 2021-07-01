package plugin

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

	gSinkmetadata = make(map[string]*uiSink)
	gSinkmetadata["taos.json"] = taosMeta
	gSinkmetadata["log.json"] = logMeta

	oldSink, err := GetSinkMeta("taos", "en_US")
	if err != nil {
		t.Errorf("%v", err)
	} else {
		if "911.911.911.911" != oldSink.Fields[0].Default {
			t.Errorf("fail")
		}
	}
}
