package main

import (
	"encoding/json"
	api "github.com/emqx/kuiper/xstream/api"
	_ "github.com/influxdata/influxdb1-client/v2"
	client "github.com/influxdata/influxdb1-client/v2"
	"log"
	"time"
)

type influxSink struct {
	url       string
	table     string
}

type ListMap []map[string]float64

func (m *influxSink) Configure(props map[string]interface{}) error {
	return nil;
}

func (m *influxSink) Open(ctx api.StreamContext) (err error) {
	logger := ctx.GetLogger()
	logger.Debug("Opening influx sink")
	return
}

func (m *influxSink) Collect(ctx api.StreamContext, data interface{}) error {
	cli := connInflux();
	logger := ctx.GetLogger()
	if v, ok := data.([]byte); ok {
		var out ListMap
		json.Unmarshal([]byte(v), &out)
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  "databasename",
			Precision: "ns", //default is ns
		})
		if err != nil {
			logger.Debug(err)
		}
		tags := map[string]string{"tagkey": "tagvalue"}
		fields := map[string]interface{}{
			"filed1": out[0]["filed1"],
			"filed2": out[0]["filed2"],
		}

		pt, err := client.NewPoint("measurement", tags, fields, time.Now())
		if err != nil {
			logger.Debug(err)
		}
		bp.AddPoint(pt)
		err = cli.Write(bp)
		if err != nil {
			logger.Debug(err)
		}
		logger.Debug("insert success")
	} else {
		logger.Debug("insert faild")
	}

	return nil
}

func (m *influxSink) Close(ctx api.StreamContext) error {
	return nil
}


func connInflux() client.Client {
	cli, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     "addrIp",
		Username: "",
		Password: "",
	})
	if err != nil {
		log.Fatal(err)
	}
	return cli
}


var Influx influxSink