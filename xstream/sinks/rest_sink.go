package sinks

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xstream/api"
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
			return fmt.Errorf("rest sink property headers %v is not a map[string]string", temp)
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
	if _, ok = common.BodyTypeMap[ms.bodyType]; !ok {
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
	_, e:= common.Send(ms.client, ms.bodyType, ms.method, ms.url, ms.headers, ms.sendSingle, v)
	return e
}

func (ms *RestSink) Close(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Closing rest sink")
	return nil
}
