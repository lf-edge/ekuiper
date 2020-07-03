package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/xstream/api"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

var BodyTypeMap = map[string]string{"none": "", "text": "text/plain", "json": "application/json", "html": "text/html", "xml": "application/xml", "javascript": "application/javascript", "form": ""}

func Send(logger api.Logger, client *http.Client, bodyType string, method string, u string, headers map[string]string, sendSingle bool, v interface{}) (*http.Response, error) {
	var req *http.Request
	var err error
	switch bodyType {
	case "none":
		req, err = http.NewRequest(method, u, nil)
		if err != nil {
			return nil, fmt.Errorf("fail to create request: %v", err)
		}
	case "json", "text", "javascript", "html", "xml":
		var body = &(bytes.Buffer{})
		switch t := v.(type) {
		case []byte:
			body = bytes.NewBuffer(t)
		default:
			return nil, fmt.Errorf("invalid content: %v", v)
		}
		req, err = http.NewRequest(method, u, body)
		if err != nil {
			return nil, fmt.Errorf("fail to create request: %v", err)
		}
		req.Header.Set("Content-Type", BodyTypeMap[bodyType])
	case "form":
		form := url.Values{}
		im, err := convertToMap(v, sendSingle)
		if err != nil {
			return nil, err
		}
		for key, value := range im {
			var vstr string
			switch value.(type) {
			case []interface{}, map[string]interface{}:
				if temp, err := json.Marshal(value); err != nil {
					return nil, fmt.Errorf("fail to parse fomr value: %v", err)
				} else {
					vstr = string(temp)
				}
			default:
				vstr = fmt.Sprintf("%v", value)
			}
			form.Set(key, vstr)
		}
		body := ioutil.NopCloser(strings.NewReader(form.Encode()))
		req, err = http.NewRequest(method, u, body)
		if err != nil {
			return nil, fmt.Errorf("fail to create request: %v", err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded;param=value")
	default:
		return nil, fmt.Errorf("unsupported body type %s", bodyType)
	}

	if len(headers) > 0 {
		for k, v := range headers {
			req.Header.Set(k, v)
		}
	}
	logger.Debugf("do request: %#v", req)
	return client.Do(req)
}

func convertToMap(v interface{}, sendSingle bool) (map[string]interface{}, error) {
	switch t := v.(type) {
	case []byte:
		r := make(map[string]interface{})
		if err := json.Unmarshal(t, &r); err != nil {
			if sendSingle {
				return nil, fmt.Errorf("fail to decode content: %v", err)
			} else {
				r["result"] = string(t)
			}
		}
		return r, nil
	default:
		return nil, fmt.Errorf("invalid content: %v", v)
	}
	return nil, fmt.Errorf("invalid content: %v", v)
}
