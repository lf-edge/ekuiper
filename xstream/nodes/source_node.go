package nodes

import (
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"fmt"
	"github.com/go-yaml/yaml"
)

type SourceNode struct {
	source api.Source
	outs   map[string]chan<- interface{}
	name   string
	ctx    api.StreamContext
	options map[string]string
}

func NewSourceNode(name string, source api.Source, options map[string]string) *SourceNode {
	return &SourceNode{
		source: source,
		outs: make(map[string]chan<- interface{}),
		name: name,
		options: options,
		ctx: nil,
	}
}

func (m *SourceNode) Open(ctx api.StreamContext, errCh chan<- error) {
	m.ctx = ctx
	logger := ctx.GetLogger()
	logger.Debugf("open source node %s with option %v", m.name, m.options)
	go func(){
		props := getConf(m.options["TYPE"], m.options["CONF_KEY"], ctx)
		err := m.source.Configure(m.options["DATASOURCE"], props)
		if err != nil{
			m.drainError(errCh, err, ctx, logger)
			return
		}
		if err := m.source.Open(ctx, func(message map[string]interface{}, meta map[string]interface{}) {
			tuple := &xsql.Tuple{Emitter: m.name, Message: message, Timestamp: common.GetNowInMilli(), Metadata: meta}
			m.Broadcast(tuple)
			logger.Debugf("%s consume data %v complete", m.name, tuple)
		}); err != nil {
			m.drainError(errCh, err, ctx, logger)
			return
		}
		for {
			select {
			case <-ctx.Done():
				logger.Infof("source %s done", m.name)
				if err := m.source.Close(ctx); err != nil {
					logger.Warnf("close source fails: %v", err)
				}
				return
			}
		}
	}()
}

func (m *SourceNode) drainError(errCh chan<- error, err error, ctx api.StreamContext, logger api.Logger) {
	select {
	case errCh <- err:
	case <-ctx.Done():
		if err := m.source.Close(ctx); err != nil {
			logger.Warnf("close source fails: %v", err)
		}
	}
	return
}

func getConf(t string, confkey string, ctx api.StreamContext) map[string]interface{} {
	logger := ctx.GetLogger()
	if t == ""{
		t = "mqtt"
	}
	confPath := "sources/" + t + ".yaml"
	if t == "mqtt"{
		confPath = "mqtt_source.yaml"
	}
	conf, err := common.LoadConf(confPath)
	props := make(map[string]interface{})
	if err == nil {
		cfg := make(map[string]map[string]interface{})
		if err := yaml.Unmarshal(conf, &cfg); err != nil {
			logger.Warnf("fail to parse yaml for source %s. Return an empty configuration", t)
		} else {
			var ok bool
			props, ok = cfg["default"]
			if !ok {
				logger.Warnf("default conf is not found", confkey)
			}
			if c, ok := cfg[confkey]; ok {
				for k, v := range c {
					props[k] = v
				}
			}
		}
	} else {
		logger.Warnf("config file %s.yaml is not loaded properly. Return an empty configuration", t)
	}
	logger.Debugf("get conf for %s with conf key %s: %v", t, confkey, props)
	return props
}

func (m *SourceNode) Broadcast(data interface{}) int{
	return Broadcast(m.outs, data, m.ctx)
}

func (m *SourceNode) GetName() string{
	return m.name
}

func (m *SourceNode) AddOutput(output chan<- interface{}, name string) (err error) {
	if _, ok := m.outs[name]; !ok{
		m.outs[name] = output
	}else{
		return fmt.Errorf("fail to add output %s, stream node %s already has an output of the same name", name, m.name)
	}
	return nil
}
