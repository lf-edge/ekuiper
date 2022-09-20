package rabbitmq

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/streadway/amqp"
)

var once sync.Once

type source struct {
	Username     string
	Password     string
	URL          string
	Exchange     string
	ExchangeType string
	RoutingKeys  []string
	conn         *amqp.Connection
	channel      *amqp.Channel
	msgs         <-chan amqp.Delivery
}

func (s *source) Configure(_ string, props map[string]interface{}) error {
	conf.Log.Infof("configurations map %#v.", props)
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
			return fmt.Errorf("Not valid addr %v.", i)
		}
	}

	if i, ok := props["exchange"]; ok {
		if e, ok := i.(string); ok {
			s.Exchange = e
		} else {
			return fmt.Errorf("Not valid exchange %v.", i)
		}
	}

	if i, ok := props["exchangetype"]; ok {
		if e, ok := i.(string); ok {
			s.ExchangeType = e
		} else {
			return fmt.Errorf("Not valid exchangeType %v.", i)
		}
	}

	if i, ok := props["routingkeys"]; ok {
		fmt.Printf("%T", i)
		switch i := i.(type) {
		case []interface{}:
			for _, value := range i {
				if r, ok := value.(string); ok {
					s.RoutingKeys = append(s.RoutingKeys, r)
				} else {
					return fmt.Errorf("Not valid routingKey %v.", i)
				}
			}
		}
	}
	conf.Log.Infof("Initialized with configurations %#v.", s)
	return nil
}

func (s *source) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	logger := ctx.GetLogger()
	ruleId := ctx.GetRuleId()

	logger.Infof("Connet to rabbitmq.")
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/", s.Username, s.Password, s.URL))
	if err != nil {
		logger.Infof("Failed to connet to rabbitmq.")
	}
	s.conn = conn

	logger.Infof("Declare a channel.")
	channel, err := s.conn.Channel()
	if err != nil {
		logger.Infof("Failed to declare a channel.")
		return
	}
	s.channel = channel

	logger.Infof("Declare a exchange.")
	if err := s.channel.ExchangeDeclarePassive(
		s.Exchange,
		s.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		logger.Infof("Failed to declare a exchange.")
		return
	}

	logger.Infof("Declare a queue.")
	queue, err := s.channel.QueueDeclare(
		ruleId,
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		logger.Infof("Failed to declare a queue.")
		return
	}

	logger.Infof("Bind a queue.")
	for _, routingKey := range s.RoutingKeys {
		fmt.Println(routingKey)
		logger.Infof("routingkey:%s", routingKey)
		if err = s.channel.QueueBind(queue.Name, string(routingKey), s.Exchange, false, nil); err != nil {
			logger.Infof("Failed bind a queue.")
			return
		}
	}

	logger.Infof("Declare a consumer.")
	msgs, err := s.channel.Consume(
		queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		logger.Infof("Failed to declare a consumer.")
		return
	}
	s.msgs = msgs

	err = s.Connect(ctx, consumer)
	if err != nil {
		errCh <- err
		return
	}
	return
}

func (s *source) Close(ctx api.StreamContext) error {
	ctx.GetLogger().Infof("Close rabbitmq source")
	s.conn.Close()
	s.channel.Close()
	return nil
}

func GetSource() *source {
	return &source{}
}

func (s *source) Connect(ctx api.StreamContext, consumer chan<- api.SourceTuple) error {
	logger := ctx.GetLogger()
	// Send data to data channel
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-s.msgs:
			var body RabbitMQMsg
			logger.Infof("body = %s\n", msg.Body)
			if err := json.Unmarshal(msg.Body, &body); err != nil {
				logger.Infof("unmarshall body error, %s\n", err.Error())
				continue
			}
			msgSplits := strings.Split(msg.RoutingKey, ".")
			msgType := msgSplits[len(msgSplits)-1]
			deviceId := msgSplits[len(msgSplits)-2]
			result := make(map[string]interface{})
			result[ctx.GetRuleId()] = body
			meta := make(map[string]interface{})
			switch msgType {
			case string(RoutingKeyDeviceProperty):
				issue_reply := msgSplits[1]
				if issue_reply == "issue_reply" {
					meta[deviceId] = body.Code
				} else {
					for _, p := range body.Params {
						meta[p.Id] = p.Value
					}
				}
				consumer <- api.NewDefaultSourceTuple(result, meta)
			case string(RoutingKeyDeviceAlarm):
				for _, p := range body.Params {
					meta[p.Id] = p.Value
				}
				consumer <- api.NewDefaultSourceTuple(result, meta)
			case string(RoutingKeyDeviceStatus):
				for _, p := range body.Params {
					meta["deviceId"] = deviceId
					meta["status"] = p.Value
				}
				consumer <- api.NewDefaultSourceTuple(result, meta)
			case string(RoutingKeyDeviceCommand):
				meta["status"] = body.Code
				consumer <- api.NewDefaultSourceTuple(result, meta)
			}
		}
	}
}
