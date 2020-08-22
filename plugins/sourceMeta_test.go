package plugins

import (
	//"net/http"
	//"net/http/httptest"
	"testing"
	// "encoding/json"
	//"fmt"
	//	"reflect"
)

func TestGetSourceMeta(t *testing.T) {
	source := &sourceProperty{
		cf: map[string]map[string]interface{}{
			"default": map[string]interface{}{
				"url": "localhost",
				"headers": map[string]interface{}{
					"Accept": "application/json",
				},
			},
		},
		meta: &sourceMeta{
			ConfKeys: map[string][]*field{
				"default": []*field{
					{
						Name:    "url",
						Default: "",
					},
					{
						Name: "headers",
						Default: []*field{
							{
								Name:    "Accept",
								Default: "",
							},
						},
					},
				},
			},
		},
	}

	g_sourceProperty = make(map[string]*sourceProperty)
	g_sourceProperty["httppull.json"] = source

	meta, err := GetSourceMeta("httppull")
	if nil != err {
		t.Error(err)
	}

	defCf := source.cf["default"]
	for _, fd := range meta.ConfKeys["default"] {
		switch fd.Name {
		case "url":
			if fd.Default != defCf["url"] {
				t.Error("url fail")
			}
		case "headers":
			head := defCf["headers"].(map[string]interface{})
			accept := head["Accept"].(string)

			meDef := fd.Default.(*[]*field)
			for _, v := range *meDef {
				if "Accept" == v.Name {
					if v.Default != accept {
						t.Error("accept fail")
					}
				}
			}
		}
	}

}
