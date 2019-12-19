package sinks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common/templates"
	"github.com/emqx/kuiper/xstream/api"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"text/template"
	"time"
)

type RestSink struct {
	method      string
	url         string
	headers     map[string]string
	bodyType    string
	timeout		int64
	sendSingle  bool
	dataTemplate string

	client      *http.Client
	tp          *template.Template
}

var methodsMap = map[string]bool{"GET": true, "HEAD": true, "POST": true, "PUT": true, "DELETE": true, "PATCH": true}
var bodyTypeMap = map[string]string{"none":"", "text": "text/plain", "json":"application/json", "html": "text/html", "xml": "application/xml", "javascript": "application/javascript", "form": ""}

func (ms *RestSink) Configure(ps map[string]interface{}) error {
	temp, ok := ps["method"]
	if ok {
		ms.method, ok = temp.(string)
		if !ok {
			return fmt.Errorf("rest sink property method %v is not a string", temp)
		}
		ms.method = strings.ToUpper(strings.Trim(ms.method, ""))
	}else{
		ms.method = "GET"
	}
	if _, ok = methodsMap[ms.method]; !ok {
		return fmt.Errorf("invalid property method: %s", ms.method)
	}
	switch ms.method{
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
	ms.url = strings.ToLower(strings.Trim(ms.url, ""))

	temp, ok = ps["headers"]
	if ok{
		ms.headers, ok = temp.(map[string]string)
		if !ok {
			return fmt.Errorf("rest sink property headers %v is not a map[string][]string", temp)
		}
	}

	temp, ok = ps["bodyType"]
	if ok{
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
	}else{
		to, ok := temp.(float64)
		if !ok {
			return fmt.Errorf("rest sink property timeout %v is not a number", temp)
		}
		ms.timeout = int64(to)
	}

	temp, ok = ps["sendSingle"]
	if !ok{
		ms.sendSingle = false
	}else{
		ms.sendSingle, ok = temp.(bool)
		if !ok {
			return fmt.Errorf("rest sink property sendSingle %v is not a bool", temp)
		}
	}

	temp, ok = ps["dataTemplate"]
	if ok{
		ms.dataTemplate, ok = temp.(string)
		if !ok {
			return fmt.Errorf("rest sink property dataTemplate %v is not a string", temp)
		}
	}

	if ms.dataTemplate != ""{
		funcMap := template.FuncMap{
			"json": templates.JsonMarshal,
		}
		temp, err := template.New("restSink").Funcs(funcMap).Parse(ms.dataTemplate)
		if err != nil{
			return fmt.Errorf("rest sink property dataTemplate %v is invalid: %v", ms.dataTemplate, err)
		}else{
			ms.tp = temp
		}
	}
	return nil
}

func (ms *RestSink) Open(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	ms.client = &http.Client{Timeout: time.Duration(ms.timeout) * time.Millisecond}
	logger.Debugf("open rest sink with configuration: {method: %s, url: %s, bodyType: %s, timeout: %d,header: %v, sendSingle: %v, dataTemplate: %s", ms.method, ms.url, ms.bodyType, ms.timeout, ms.headers, ms.sendSingle, ms.dataTemplate)
	return nil
}

func (ms *RestSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	v, ok := item.([]byte)
	if !ok {
		logger.Warnf("rest sink receive non []byte data: %v", item)
	}
	logger.Debugf("rest sink receive %s", item)
	if !ms.sendSingle{
		return ms.send(v, logger)
	}else{
		j, err := extractInput(v)
		if err != nil {
			return err
		}
		logger.Debugf("receive %d records", len(j))
		for _, r := range j {
			ms.send(r, logger)
		}
	}
	return nil
}

func extractInput(v []byte) ([]map[string]interface{}, error) {
	var j []map[string]interface{}
	if err := json.Unmarshal(v, &j); err != nil {
		return nil, fmt.Errorf("fail to decode the input %s as json: %v", v, err)
	}
	return j, nil
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
			if ms.tp != nil {
				j, err := extractInput(t)
				if err != nil {
					return err
				}
				err = ms.tp.Execute(body, j)
				if err != nil{
					return fmt.Errorf("fail to decode content: %v", err)
				}
			}else{
				body = bytes.NewBuffer(t)
			}
		case map[string]interface{}:
			if ms.tp != nil{
				err = ms.tp.Execute(body, t)
				if err != nil{
					return fmt.Errorf("fail to decode content: %v", err)
				}
			}else{
				content, err := json.Marshal(t)
				if err != nil{
					return fmt.Errorf("fail to decode content: %v", err)
				}
				body = bytes.NewBuffer(content)
			}
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
		im, err := convertToMap(v, ms.tp)
		if err != nil {
			return err
		}
		for key, value := range im {
			var vstr string
			switch value.(type) {
			case []interface{}, map[string]interface{}:
				if temp, err := json.Marshal(value); err != nil {
					return fmt.Errorf("fail to parse fomr value: %v", err)
				}else{
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
	}
	return nil
}

func convertToMap(v interface{}, tp *template.Template) (map[string]interface{}, error) {
	switch t := v.(type) {
	case []byte:
		if tp != nil{
			j, err := extractInput(t)
			if err != nil {
				return nil, err
			}
			var output bytes.Buffer
			err = tp.Execute(&output, j)
			if err != nil{
				return nil, fmt.Errorf("fail to decode content: %v", err)
			}
			r := make(map[string]interface{})
			if err := json.Unmarshal(output.Bytes(), &r); err != nil{
				return nil, fmt.Errorf("fail to decode content: %v", err)
			}else{
				return r, nil
			}
		}else{
			r := make(map[string]interface{})
			r["result"] = string(t)
			return r, nil
		}
	case map[string]interface{}:
		if tp != nil{
			var output bytes.Buffer
			err := tp.Execute(&output, t)
			if err != nil{
				return nil, fmt.Errorf("fail to decode content: %v", err)
			}
			r := make(map[string]interface{})
			if err := json.Unmarshal(output.Bytes(), &r); err != nil{
				return nil, fmt.Errorf("fail to decode content: %v", err)
			}else{
				return r, nil
			}
		}else{
			return t, nil
		}
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