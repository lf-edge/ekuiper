package sinks

import (
	"github.com/emqx/kuiper/xstream/api"
)

type NopSink struct {
	log bool
}

func (ns *NopSink) Configure(ps map[string]interface{}) error {
	var log = false
	l, ok := ps["log"]
	if ok {
		log = l.(bool)
	}
	ns.log = log
	return nil
}

func (ns *NopSink) Open(ctx api.StreamContext) error {
	return nil
}

func (ns *NopSink) Collect(ctx api.StreamContext, item interface{}) error {
	logger := ctx.GetLogger()
	if ns.log {
		logger.Infof("%s", item)
	}
	return nil
}

func (ns *NopSink) Close(ctx api.StreamContext) error {
	return nil
}
