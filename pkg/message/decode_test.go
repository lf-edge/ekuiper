package message

import (
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"path"
	"reflect"
	"testing"
)

func TestMessageDecode(t *testing.T) {
	image, err := ioutil.ReadFile(path.Join("../../docs", "cover.jpg"))
	if err != nil {
		t.Errorf("Cannot read image: %v", err)
	}
	b64img := base64.StdEncoding.EncodeToString(image)
	var tests = []struct {
		payload []byte
		format  string
		result  map[string]interface{}
	}{
		{
			payload: image,
			format:  "binary",
			result: map[string]interface{}{
				"self": image,
			},
		}, {
			payload: []byte(fmt.Sprintf(`{"format":"jpg","content":"%s"}`, b64img)),
			format:  "json",
			result: map[string]interface{}{
				"format":  "jpg",
				"content": b64img,
			},
		},
	}
	for i, tt := range tests {
		result, err := Decode(tt.payload, tt.format)
		if err != nil {
			t.Errorf("%d decode error: %v", i, err)
		}
		if !reflect.DeepEqual(tt.result, result) {
			t.Errorf("%d result mismatch:\n\nexp=%s\n\ngot=%s\n\n", i, tt.result, result)
		}
	}
}
