// Copyright 2021-2025 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package influx

import (
	"crypto/tls"
	"fmt"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/tspoint"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/cert"
)

// c is the configuration for influx2 sink
type c struct {
	// connection
	Addr     string `json:"addr"`
	Username string `json:"username"`
	Password string `json:"password"`
	// http connection
	// tls conf in cert.go
	// write options
	Database    string `json:"database"`
	Measurement string `json:"measurement"`
	tspoint.WriteOptions
}

type influxSink struct {
	conf    c
	tlsconf *tls.Config
	// temp variables
	bp  client.BatchPoints
	cli client.Client
}

func (m *influxSink) Provision(ctx api.StreamContext, props map[string]any) error {
	m.conf = c{
		WriteOptions: tspoint.WriteOptions{
			PrecisionStr: "ms",
		},
	}
	err := cast.MapToStruct(props, &m.conf)
	if err != nil {
		return fmt.Errorf("error configuring influx2 sink: %s", err)
	}
	if len(m.conf.Addr) == 0 {
		return fmt.Errorf("addr is required")
	}
	if len(m.conf.Database) == 0 {
		return fmt.Errorf("database is required")
	}
	if len(m.conf.Measurement) == 0 {
		return fmt.Errorf("measurement is required")
	}
	err = cast.MapToStruct(props, &m.conf.WriteOptions)
	if err != nil {
		return fmt.Errorf("error configuring influx sink: %s", err)
	}
	err = m.conf.WriteOptions.Validate()
	if err != nil {
		return err
	}
	tlsConf, err := cert.GenTLSConfig(ctx, props)
	if err != nil {
		return fmt.Errorf("error configuring tls: %s", err)
	}
	m.tlsconf = tlsConf
	return nil
}

func (m *influxSink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) (err error) {
	var insecureSkip bool
	if m.tlsconf != nil {
		insecureSkip = m.tlsconf.InsecureSkipVerify
	}
	defer func() {
		if err != nil {
			sch(api.ConnectionDisconnected, err.Error())
		} else {
			sch(api.ConnectionConnected, "")
		}
	}()
	m.cli, err = client.NewHTTPClient(client.HTTPConfig{
		Addr:               m.conf.Addr,
		Username:           m.conf.Username,
		Password:           m.conf.Password,
		InsecureSkipVerify: insecureSkip,
		TLSConfig:          m.tlsconf,
	})
	if err != nil {
		return fmt.Errorf("error creating influx client: %s", err)
	}
	err = m.conf.WriteOptions.ValidateTagTemplates(ctx)
	if err != nil {
		return err
	}
	_, _, err = m.cli.Ping(time.Second * 10)
	if err != nil {
		return fmt.Errorf("error pinging influx server: %s", err)
	}
	m.bp, err = client.NewBatchPoints(client.BatchPointsConfig{
		Database:  m.conf.Database,
		Precision: m.conf.PrecisionStr,
	})
	return err
}

func (m *influxSink) Ping(ctx api.StreamContext, props map[string]any) (err error) {
	if err = m.Provision(ctx, props); err != nil {
		return err
	}
	var insecureSkip bool
	if m.tlsconf != nil {
		insecureSkip = m.tlsconf.InsecureSkipVerify
	}
	m.cli, err = client.NewHTTPClient(client.HTTPConfig{
		Addr:               m.conf.Addr,
		Username:           m.conf.Username,
		Password:           m.conf.Password,
		InsecureSkipVerify: insecureSkip,
		TLSConfig:          m.tlsconf,
	})
	if err != nil {
		return fmt.Errorf("error creating influx client: %s", err)
	}
	defer func() {
		if m.cli != nil {
			m.cli.Close()
		}
	}()
	// Test connection. Put it here to avoid server connection when running test in Configure
	_, _, err = m.cli.Ping(time.Second * 10)
	return err
}

func (m *influxSink) Collect(ctx api.StreamContext, item api.MessageTuple) error {
	return m.collect(ctx, item.ToMap())
}

func (m *influxSink) CollectList(ctx api.StreamContext, items api.MessageTupleList) error {
	return m.collect(ctx, items.ToMaps())
}

func (m *influxSink) collect(ctx api.StreamContext, data any) error {
	logger := ctx.GetLogger()
	err := m.transformPoints(ctx, data)
	if err != nil {
		logger.Error(err)
		return err
	}
	// Write the batch
	err = m.cli.Write(m.bp)
	if err != nil {
		logger.Error(err)
		return err
	}
	logger.Debug("influx insert success")
	return nil
}

func (m *influxSink) transformPoints(ctx api.StreamContext, data any) error {
	var err error
	m.bp, err = client.NewBatchPoints(client.BatchPointsConfig{
		Database:  m.conf.Database,
		Precision: m.conf.PrecisionStr,
	})
	if err != nil {
		return err
	}

	rawPts, err := tspoint.SinkTransform(ctx, data, &m.conf.WriteOptions)
	if err != nil {
		ctx.GetLogger().Error(err)
		return err
	}
	for _, rawPt := range rawPts {
		pt, err := client.NewPoint(m.conf.Measurement, rawPt.Tags, rawPt.Fields, rawPt.Tt)
		if err != nil {
			return err
		}
		m.bp.AddPoint(pt)
	}
	return nil
}

func (m *influxSink) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("influx sink close")
	return m.cli.Close()
}

func GetSink() api.Sink {
	return &influxSink{}
}

var (
	_ api.TupleCollector = &influxSink{}
	_ util.PingableConn  = &influxSink{}
)
