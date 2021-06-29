// +build plugins

package main

import (
	"encoding/json"
	api "github.com/emqx/kuiper/xstream/api"
	_ "github.com/influxdata/influxdb1-client/v2"
	client "github.com/influxdata/influxdb1-client/v2"
	"strings"
	"time"
)

type influxSink struct {
	addr         string
	username     string
	password     string
	measurement  string
	databasename string
	tagkey       string
	tagvalue     string
	fields       string
	cli          client.Client
	fieldmap     map[string]interface{}
}

type ListMap []map[string]interface{}

func (m *influxSink) Configure(props map[string]interface{}) error {
	if i, ok := props["addr"]; ok {
		if i, ok := i.(string); ok {
			m.addr = i
		}
	}
	if i, ok := props["username"]; ok {
		if i, ok := i.(string); ok {
			m.username = i
		}
	}
	if i, ok := props["password"]; ok {
		if i, ok := i.(string); ok {
			m.password = i
		}
	}
	if i, ok := props["measurement"]; ok {
		if i, ok := i.(string); ok {
			m.measurement = i
		}
	}
	if i, ok := props["databasename"]; ok {
		if i, ok := i.(string); ok {
			m.databasename = i
		}
	}
	if i, ok := props["tagkey"]; ok {
		if i, ok := i.(string); ok {
			m.tagkey = i
		}
	}
	if i, ok := props["tagvalue"]; ok {
		if i, ok := i.(string); ok {
			m.tagvalue = i
		}
	}
	if i, ok := props["fields"]; ok {
		if i, ok := i.(string); ok {
			m.fields = i
		}
	}
	return nil
}

func (m *influxSink) Open(ctx api.StreamContext) (err error) {
	logger := ctx.GetLogger()
	logger.Debug("Opening influx sink")
	m.cli, err = client.NewHTTPClient(client.HTTPConfig{
		Addr:     m.addr,
		Username: m.username,
		Password: m.password,
	})
	if err != nil {
		logger.Debug(err)
		return err
	}
	return nil
}

func (m *influxSink) Collect(ctx api.StreamContext, data interface{}) error {
	logger := ctx.GetLogger()

	if v, ok := data.([]byte); ok {
		var out ListMap
		if err := json.Unmarshal([]byte(v), &out); err != nil {
			logger.Debug("Failed to unmarshal data with error %s.\n", err)
			return err
		}
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  m.databasename,
			Precision: "ns",
		})
		if err != nil {
			logger.Debug(err)
			return err
		}
		tags := map[string]string{m.tagkey: m.tagvalue}
		fields := strings.Split(m.fields, ",")
		m.fieldmap = make(map[string]interface{}, 10)
		for _, field := range fields {
			m.fieldmap[field] = out[0][field]
		}

		pt, err := client.NewPoint(m.measurement, tags, m.fieldmap, time.Now())
		if err != nil {
			logger.Debug(err)
			return err
		}
		bp.AddPoint(pt)
		err = m.cli.Write(bp)
		if err != nil {
			logger.Debug(err)
			return err
		}
		logger.Debug("insert success")
	} else {
		logger.Debug("insert failed")
	}
	return nil
}

func (m *influxSink) Close(ctx api.StreamContext) error {
	m.cli.Close()
	return nil
}

func Influx() api.Sink {
	return &influxSink{}
}
