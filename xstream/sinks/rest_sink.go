package sinks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/xstream/api"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type RestSink struct {
	method     string
	url        string
	headers    map[string]string
	bodyType   string
	timeout    int64
	sendSingle bool

	client *http.Client
}

var methodsMap = map[string]bool{"GET": true, "HEAD": true, "POST": true, "PUT": true, "DELETE": true, "PATCH": true}
var bodyTypeMap = map[string]string{"none": "", "text": "text/plain", "json": "application/json", "html": "text/html", "xml": "application/xml", "javascript": "application/javascript", "form": ""}

func (ms *RestSink) Configure(ps map[string]interface{}) error {
	temp, ok := ps["method"]
	if ok {
		ms.method, ok = temp.(string)
		if !ok {
			return fmt.Errorf("rest sink property method %v is not a string", temp)
		}
		ms.method = strings.ToUpper(strings.Trim(ms.method, ""))
	} else {
		ms.method = "GET"
	}
	if _, ok = methodsMap[ms.method]; !ok {
		return fmt.Errorf("invalid property method: %s", ms.method)
	}
	switch ms.method {
	case "GET", "HEAD":
		ms.bodyType = "none"
	default:
		ms.bodyType = "json"
	}

	temp, ok = ps["url"]
	if !ok {
		return fmt.Errorf("rest sink is missing property url")
	}
	ms.url, ok = temp.(string)
	if !ok {
		return fmt.Errorf("rest sink property url %v is not a string", temp)
	}
	ms.url = strings.Trim(ms.url, "")

	temp, ok = ps["headers"]
	if ok {
		ms.headers, ok = temp.(map[string]string)
		if !ok {
			return fmt.Errorf("rest sink property headers %v is not a map[string][]string", temp)
		}
	}

	temp, ok = ps["bodyType"]
	if ok {
		ms.bodyType, ok = temp.(string)
		if !ok {
			return fmt.Errorf("rest sink property bodyType %v is not a string", temp)
		}
		ms.bodyType = strings.ToLower(strings.Trim(ms.bodyType, ""))
	}
	if _, ok = bodyTypeMap[ms.bodyType]; !ok {
		return fmt.Errorf("invalid property bodyType: %s, should be \"none\" or \"form\"", ms.bodyType)
	}

	temp, ok = ps["timeout"]
	if !ok {
		ms.timeout = 5000
	} else {
		to, ok := temp.(float64)
		if !ok {
			return fmt.Errorf("rest sink property timeout %v is not a number", temp)
		}
		ms.timeout = int64(to)
	}

	temp, ok = ps["sendSingle"]
	if !ok {
		ms.sendSingle = false
	} else {
		ms.sendSingle, ok = temp.(bool)
		if !ok {
			return fmt.Errorf("rest sink property sendSingle %v is not a bool", temp)
		}
	}

	return nil
}

func (ms *RestSink) Open(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	ms.client = &http.Client{Timeout: time.Duration(ms.timeout) * time.Millisecond}
	logger.Infof("open rest sink with configuration: {method: %s, url: %s, bodyType: %s, timeout: %d,header: %v, sendSingle: %v", ms.method, ms.url, ms.bodyType, ms.timeout, ms.headers, ms.sendSingle)

	timeout := 1 * time.Second
	if u, err := url.Parse(ms.url); err != nil {
		return err
	} else {
		_, err := net.DialTimeout("tcp", u.Host, timeout)
		if err != nil {
			logger.Errorf("Target web server unreachable: %s", err)
			return err
		} else {
			logger.Infof("Target web server is available.")
		}
	}
	return nil
}

type MultiErrors []error

func (me MultiErrors) AddError(err error) MultiErrors {
	me = append(me, err)
	return me
}

func (me MultiErrors) Error() string {
	s := make([]string, len(me))
	for i, v := range me {
		s = append(s, fmt.Sprintf("Error %d with info %s. \n", i, v))
	}
	return strings.Join(s, "  ")
}

func (ms *RestSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	v, ok := item.([]byte)
	if !ok {
		logger.Warnf("rest sink receive non []byte data: %v", item)
	}
	logger.Debugf("rest sink receive %s", item)
	return ms.send(v, logger)
}

func (ms *RestSink) send(v interface{}, logger api.Logger) error {
	var req *http.Request
	var err error
	switch ms.bodyType {
	case "none":
		req, err = http.NewRequest(ms.method, ms.url, nil)
		if err != nil {
			return fmt.Errorf("fail to create request: %v", err)
		}
	case "json", "text", "javascript", "html", "xml":
		var body = &(bytes.Buffer{})
		switch t := v.(type) {
		case []byte:
			body = bytes.NewBuffer(t)
		default:
			return fmt.Errorf("invalid content: %v", v)
		}
		req, err = http.NewRequest(ms.method, ms.url, body)
		if err != nil {
			return fmt.Errorf("fail to create request: %v", err)
		}
		req.Header.Set("Content-Type", bodyTypeMap[ms.bodyType])
	case "form":
		form := url.Values{}
		im, err := convertToMap(v, ms.sendSingle)
		if err != nil {
			return err
		}
		for key, value := range im {
			var vstr string
			switch value.(type) {
			case []interface{}, map[string]interface{}:
				if temp, err := json.Marshal(value); err != nil {
					return fmt.Errorf("fail to parse fomr value: %v", err)
				} else {
					vstr = string(temp)
				}
			default:
				vstr = fmt.Sprintf("%v", value)
			}
			form.Set(key, vstr)
		}
		body := ioutil.NopCloser(strings.NewReader(form.Encode()))
		req, err = http.NewRequest(ms.method, ms.url, body)
		if err != nil {
			return fmt.Errorf("fail to create request: %v", err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded;param=value")
	default:
		return fmt.Errorf("unsupported body type %s", ms.bodyType)
	}

	if len(ms.headers) > 0 {
		for k, v := range ms.headers {
			req.Header.Set(k, v)
		}
	}
	logger.Debugf("do request: %s %s with %s", ms.method, ms.url, req.Body)
	resp, err := ms.client.Do(req)
	if err != nil {
		return fmt.Errorf("rest sink fails to send out the data")
	} else {
		logger.Debugf("rest sink got response %v", resp)
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return fmt.Errorf("rest sink fails to err http return code: %d.", resp.StatusCode)
		}
	}
	return nil
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

func (ms *RestSink) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing rest sink")
	return nil
}
