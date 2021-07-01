package sinks

import (
	"crypto/tls"
	"fmt"
	"github.com/emqx/kuiper/internal/pkg/httpx"
	"github.com/emqx/kuiper/pkg/api"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type RestSink struct {
	method             string
	url                string
	headers            map[string]string
	bodyType           string
	timeout            int64
	sendSingle         bool
	debugResp          bool
	insecureSkipVerify bool

	client *http.Client
}

var methodsMap = map[string]bool{"GET": true, "HEAD": true, "POST": true, "PUT": true, "DELETE": true, "PATCH": true}

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
		ms.headers = make(map[string]string)
		if m, ok := temp.(map[string]interface{}); ok {
			for k, v := range m {
				if v1, ok1 := v.(string); ok1 {
					ms.headers[k] = v1
				} else {
					return fmt.Errorf("header value %s for header %s is not a string", v, k)
				}
			}
		} else {
			return fmt.Errorf("rest sink property headers %v is not a map[string]interface", temp)
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
	if _, ok = httpx.BodyTypeMap[ms.bodyType]; !ok {
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

	temp, ok = ps["debugResp"]
	if !ok {
		ms.debugResp = false
	} else {
		ms.debugResp, ok = temp.(bool)
		if !ok {
			return fmt.Errorf("rest sink property debugResp %v is not a bool", temp)
		}
	}

	temp, ok = ps["insecureSkipVerify"]
	if !ok {
		ms.insecureSkipVerify = true
	} else {
		ms.insecureSkipVerify, ok = temp.(bool)
		if !ok {
			return fmt.Errorf("rest sink property insecureSkipVerify %v is not a bool", temp)
		}
	}
	return nil
}

func (ms *RestSink) Open(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: ms.insecureSkipVerify},
	}
	ms.client = &http.Client{
		Transport: tr,
		Timeout:   time.Duration(ms.timeout) * time.Millisecond}
	logger.Infof("open rest sink with configuration: {method: %s, url: %s, bodyType: %s, timeout: %d,header: %v, sendSingle: %v, insecureSkipVerify: %v", ms.method, ms.url, ms.bodyType, ms.timeout, ms.headers, ms.sendSingle, ms.insecureSkipVerify)

	if _, err := url.Parse(ms.url); err != nil {
		return err
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
	resp, err := ms.Send(v, logger)
	if err != nil {
		return fmt.Errorf("rest sink fails to send out the data: %s", err)
	} else {
		logger.Debugf("rest sink got response %v", resp)
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			buf, _ := ioutil.ReadAll(resp.Body)
			logger.Errorf("%s\n", string(buf))
			return fmt.Errorf("rest sink fails to err http return code: %d and error message %s.", resp.StatusCode, string(buf))
		} else {
			if ms.debugResp {
				if buf, bodyErr := ioutil.ReadAll(resp.Body); bodyErr != nil {
					logger.Errorf("%s\n", bodyErr)
				} else {
					logger.Infof("Response content: %s\n", string(buf))
				}
			}
		}
	}
	return nil
}

func (ms *RestSink) Send(v interface{}, logger api.Logger) (*http.Response, error) {
	return httpx.Send(logger, ms.client, ms.bodyType, ms.method, ms.url, ms.headers, ms.sendSingle, v)
}

func (ms *RestSink) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing rest sink")
	return nil
}
