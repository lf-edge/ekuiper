package main

import (
	"context"
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/message"
	zmq "github.com/pebbe/zmq4"
)

type zmqSource struct {
	subscriber    *zmq.Socket
	srv           string
	topic         string
	messageFormat string
	cancel        context.CancelFunc
}

func (s *zmqSource) Configure(topic string, props map[string]interface{}) error {
	s.topic = topic
	srv, ok := props["server"]
	if !ok {
		return fmt.Errorf("zmq source is missing property server")
	}
	s.srv = srv.(string)
	f, ok := props["format"]
	if !ok {
		s.messageFormat = message.FormatJson
	} else {
		s.messageFormat = f.(string)
	}
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
	logger.Debugf("start to listen")
	for {
		msgs, err := s.subscriber.RecvMessageBytes(0)
		if err != nil {
			id, err := s.subscriber.GetIdentity()
			errCh <- fmt.Errorf("zmq source getting message %s error: %v", id, err)
		} else {
			logger.Debugf("zmq source receive %v", msgs)
			var m []byte
			for i, msg := range msgs {
				if i == 0 && s.topic != "" {
					continue
				}
				m = append(m, msg...)
			}
			meta := make(map[string]interface{})
			if s.topic != "" {
				meta["topic"] = string(msgs[0])
			}
			result, e := message.Decode(m, s.messageFormat)
			if e != nil {
				logger.Errorf("Invalid data format, cannot decode %v to %s format with error %s", m, s.messageFormat, e)
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
}

func (s *zmqSource) Close(ctx api.StreamContext) error {
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}

func Zmq() api.Source {
	return &zmqSource{}
}
