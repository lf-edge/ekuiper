package rabbitmq

import (
	"encoding/json"
	"fmt"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/streadway/amqp"
)

type sink struct {
	Username     string
	Password     string
	URL          string
	Exchange     string
	ExchangeType string
	RoutingKey   string
	conn         *amqp.Connection
	channel      *amqp.Channel
}

func (s *sink) Configure(props map[string]interface{}) error {
	if i, ok := props["username"]; ok {
		if u, ok := i.(string); ok {
			s.Username = u
		} else {
			return fmt.Errorf("Not valid username %v.", i)
		}
	}

	if i, ok := props["password"]; ok {
		if p, ok := i.(string); ok {
			s.Password = p
		} else {
			return fmt.Errorf("Not valid password %v.", i)
		}
	}

	if i, ok := props["url"]; ok {
		if u, ok := i.(string); ok {
			s.URL = u
		} else {
			return fmt.Errorf("Not valid url %v.", i)
		}
	}

	if i, ok := props["exchange"]; ok {
		if e, ok := i.(string); ok {
			s.Exchange = e
		} else {
			return fmt.Errorf("Not valid exchange %v.", i)
		}
	}

	if i, ok := props["exchangeType"]; ok {
		if e, ok := i.(string); ok {
			s.ExchangeType = e
		} else {
			return fmt.Errorf("Not valid exchangeType %v.", i)
		}
	}

	if i, ok := props["routingKey"]; ok {
		if r, ok := i.(string); ok {
			s.RoutingKey = r
		} else {
			return fmt.Errorf("Not valid routingKey %v.", i)
		}
	}

	conf.Log.Debugf("Initialized with configurations %#v.", s)
	return nil
}

func (s *sink) Open(ctx api.StreamContext) error {
	logger := ctx.GetLogger()
	logger.Infof("Sink connet to rabbitmq.")
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", s.Username, s.Password, s.URL))
	if err != nil {
		return err
	}
	s.conn = conn
	logger.Infof("Sink declare a channel.")
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	s.channel = channel
	return nil
}

func (s *sink) Collect(ctx api.StreamContext, data interface{}) error {
	if err := s.channel.ExchangeDeclare(
		s.Exchange,
		s.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	if err := s.channel.Publish(
		s.Exchange,
		string(s.RoutingKey),
		false,
		false,
		amqp.Publishing{
			Body: bytes,
		},
	); err != nil {
		return err
	}

	return nil
}

func (s *sink) Close(ctx api.StreamContext) error {
	s.channel.Close()
	s.conn.Close()
	return nil
}

func GetSink() *sink {
	return &sink{}
}
