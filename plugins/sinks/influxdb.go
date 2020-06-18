package main

import (
	"encoding/json"
	api "github.com/emqx/kuiper/xstream/api"
	_ "github.com/influxdata/influxdb1-client/v2"
	client "github.com/influxdata/influxdb1-client/v2"
	"time"
)

type influxSink struct {
	addr string
	username string
	password string
	measurement string
	databasename string
	tagkey string
	tagvalue string
}

var cli client.Client

type ListMap []map[string]float64

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
	return nil;
}

func (m *influxSink) Open(ctx api.StreamContext) (err error) {
	logger := ctx.GetLogger();
	logger.Debug("Opening influx sink")
	cli, err = client.NewHTTPClient(client.HTTPConfig{
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
		json.Unmarshal([]byte(v), &out)
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  m.databasename,
			Precision: "ns", //default is ns
		})
		if err != nil {
			logger.Debug(err)
			return err
		}
		tags := map[string]string{m.tagkey: m.tagvalue}
		fields := map[string]interface{}{
			"temperature" : out[0]["temperature"],
			"humidity": out[0]["humidity"],
		}

		pt, err := client.NewPoint(m.measurement, tags, fields, time.Now())
		if err != nil {
			logger.Debug(err)
			return err
		}
		bp.AddPoint(pt)
		err = cli.Write(bp)
		if err != nil {
			logger.Debug(err)
			return err
		}
		logger.Debug("insert success")
	} else {
		logger.Debug("insert faild")
	}
	return nil
}

func (m *influxSink) Close(ctx api.StreamContext) error {
	// Close the client
	cli.Close();
	return nil
}

var Influx influxSink