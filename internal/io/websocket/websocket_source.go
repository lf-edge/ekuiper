// Copyright 2023 EMQ Technologies Co., Ltd.
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

package websocket

import (
	"fmt"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/io"
	"github.com/lf-edge/ekuiper/internal/topo/connection/clients"
	"github.com/lf-edge/ekuiper/internal/topo/context"
	"github.com/lf-edge/ekuiper/internal/xsql"
	"github.com/lf-edge/ekuiper/pkg/api"
)

type WebsocketSource struct {
	props map[string]interface{}
	cli   api.MessageClient
}

func (wss *WebsocketSource) Ping(dataSource string, props map[string]interface{}) error {
	if err := wss.Configure(dataSource, props); err != nil {
		return err
	}
	cli, err := clients.GetClient("websocket", wss.props)
	if err != nil {
		return err
	}
	defer clients.ReleaseClient(context.Background(), cli)
	return cli.Ping()
}

func (wss *WebsocketSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	cli, err := clients.GetClient("websocket", wss.props)
	if err != nil {
		errCh <- err
	}
	wss.cli = cli
	if err := wss.subscribe(ctx, consumer); err != nil {
		errCh <- err
	}
}

func (wss *WebsocketSource) subscribe(ctx api.StreamContext, consumer chan<- api.SourceTuple) error {
	log := ctx.GetLogger()
	messages := make(chan interface{}, 1024)
	topics := []api.TopicChannel{{Topic: "", Messages: messages}}
	errCh := make(chan error, len(topics))
	if err := wss.cli.Subscribe(ctx, topics, errCh, nil); err != nil {
		return err
	}
	var tuples []api.SourceTuple
	for {
		select {
		case <-ctx.Done():
			log.Info("Exit subscription to websocket source")
			return nil
		case err := <-errCh:
			tuples = []api.SourceTuple{
				&xsql.ErrorSourceTuple{
					Error: fmt.Errorf("the subscription to websocket source have error %s.\n", err.Error()),
				},
			}
		case msg, ok := <-messages:
			if !ok { // the source is closed
				log.Info("Exit subscription to websocket source.")
				return nil
			}
			dataTuples, err := buildTuples(ctx, msg)
			if err != nil {
				tuples = []api.SourceTuple{
					&xsql.ErrorSourceTuple{
						Error: fmt.Errorf("the subscription to websocket source have error %s.\n", err.Error()),
					},
				}
			} else {
				tuples = dataTuples
			}
		}
		io.ReceiveTuples(ctx, consumer, tuples)
	}
}

func buildTuples(ctx api.StreamContext, msg interface{}) ([]api.SourceTuple, error) {
	msgBytes, ok := msg.([]byte)
	if !ok {
		return nil, fmt.Errorf("websocker source should recv bytes")
	}
	dataLists, err := ctx.DecodeIntoList(msgBytes)
	if err != nil {
		return nil, err
	}
	rcvTime := conf.GetNow()
	tuples := make([]api.SourceTuple, 0, len(dataLists))
	for _, data := range dataLists {
		tuples = append(tuples, api.NewDefaultSourceTupleWithTime(data, nil, rcvTime))
	}
	return tuples, nil
}

func (wss *WebsocketSource) Configure(datasource string, props map[string]interface{}) error {
	props["path"] = datasource
	wss.props = props
	return nil
}

func (wss *WebsocketSource) Validate(props map[string]interface{}) error {
	return nil
}

func (wss *WebsocketSource) Close(ctx api.StreamContext) error {
	clients.ReleaseClient(ctx, wss.cli)
	return nil
}
