package main

import (
"encoding/json"
api "github.com/emqx/kuiper/xstream/api"
_ "github.com/influxdata/influxdb1-client/v2"
client "github.com/influxdata/influxdb1-client/v2"
"log"
"time"
)

/**
You can specify the URL，table-name in the configuration file.
But now I write in this code。
 */

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
		var saveData ListMap
		json.Unmarshal([]byte(v), &saveData)
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			//this is database name
			Database:  "databasename",
			//default is ns
			Precision: "ns",
		})
		if err != nil {
			logger.Debug(err)
		}
		//Influx insrt sql （insert test,host=127.0.0.1,monitor_name=test, humidity=25 temperature=20）
		tags := map[string]string{"moniter_name": "test"}
		fields := map[string]interface{}{
			"humidity":   saveData[0]["humidity"],
			"temperature": saveData[0]["temperature"],
		}

		pt, err := client.NewPoint("test", tags, fields, time.Now())
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

/**
  Addr is InfluxDB IP
*/
func connInflux() client.Client {
	cli, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     "http://149.28.121.58:8086",
		Username: "admin",
		Password: "",
	})
	if err != nil {
		log.Fatal(err)
	}
	return cli
}


var Influx influxSink