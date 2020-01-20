package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/xstream/api"
	zmq "github.com/pebbe/zmq4"
)

type zmqSource struct {
	subscriber *zmq.Socket
	srv        string
	topic      string
	cancel     context.CancelFunc
}

func (s *zmqSource) Configure(topic string, props map[string]interface{}) error {
	s.topic = topic
	srv, ok := props["server"]
	if !ok {
		return fmt.Errorf("zmq source is missing property server")
	}
	s.srv = srv.(string)
	return nil
}

func (s *zmqSource) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	logger := ctx.GetLogger()
	var err error
	s.subscriber, err = zmq.NewSocket(zmq.SUB)
	if err != nil {
		errCh <- fmt.Errorf("zmq source fails to create socket: %v", err)
	}
	err = s.subscriber.Connect(s.srv)
	if err != nil {
		errCh <- fmt.Errorf("zmq source fails to connect to %s: %v", s.srv, err)
	}
	s.subscriber.SetSubscribe(s.topic)
	logger.Debugf("zmq source subscribe to topic %s", s.topic)
	exeCtx, cancel := ctx.WithCancel()
	s.cancel = cancel
	go func(exeCtx api.StreamContext) {
		logger.Debugf("start to listen")
		for {
			msgs, err := s.subscriber.RecvMessage(0)
			if err != nil {
				id, err := s.subscriber.GetIdentity()
				errCh <- fmt.Errorf("zmq source getting message %s error: %v", id, err)
			} else {
				logger.Debugf("zmq source receive %v", msgs)
				var m string
				for i, msg := range msgs {
					if i == 0 && s.topic != "" {
						continue
					}
					m += msg
				}
				meta := make(map[string]interface{})
				if s.topic != "" {
					meta["topic"] = msgs[0]
				}
				result := make(map[string]interface{})
				if e := json.Unmarshal([]byte(m), &result); e != nil {
					logger.Warnf("zmq source message %s is not json", m)
				} else {
					consumer <- api.NewDefaultSourceTuple(result, meta)
				}
			}
			select {
			case <-exeCtx.Done():
				logger.Infof("zmq source done")
				if s.subscriber != nil {
					s.subscriber.Close()
				}
				return
			default:
				//do nothing
			}
		}
	}(exeCtx)
}

func (s *zmqSource) Close(ctx api.StreamContext) error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

var Zmq zmqSource
