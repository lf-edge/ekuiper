package main

import (
	"fmt"
	"github.com/edgexfoundry/go-mod-messaging/messaging"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"github.com/emqx/kuiper/common"
	"os"
)

func subEventsFromZMQ() {
	var msgConfig1 = types.MessageBusConfig{
		SubscribeHost: types.HostInfo{
			Host:     "localhost",
			Port:     5571,
			Protocol: "tcp",
		},
		Type:messaging.ZeroMQ,
	}

	if msgClient, err := messaging.NewMessageClient(msgConfig1); err != nil {
		common.Log.Fatal(err)
	} else {
		if ec := msgClient.Connect(); ec != nil {
			common.Log.Fatal(ec)
		} else {
			//log.Infof("The connection to edgex messagebus is established successfully.")
			messages := make(chan types.MessageEnvelope)
			topics := []types.TopicChannel{{Topic: "", Messages: messages}}
			err := make(chan error)
			if e := msgClient.Subscribe(topics, err); e != nil {
				//log.Errorf("Failed to subscribe to edgex messagebus topic %s.\n", e)
				common.Log.Fatal(e)
			} else {
				var count int = 0
				for {
					select {
					case e1 := <-err:
						common.Log.Errorf("%s\n", e1)
						return
					case env := <-messages:
						count ++
						fmt.Printf("%s\n", env.Payload)
						if count == 1 {
							return
						}
					}
				}
			}
		}
	}
}

func subEventsFromMQTT(host string) {
	var msgConfig1 = types.MessageBusConfig{
		SubscribeHost: types.HostInfo{
			Host:     host,
			Port:     1883,
			Protocol: "tcp",
		},
		Type: messaging.MQTT,
	}

	if msgClient, err := messaging.NewMessageClient(msgConfig1); err != nil {
		common.Log.Fatal(err)
	} else {
		if ec := msgClient.Connect(); ec != nil {
			common.Log.Fatal(ec)
		} else {
			//log.Infof("The connection to edgex messagebus is established successfully.")
			messages := make(chan types.MessageEnvelope)
			topics := []types.TopicChannel{{Topic: "result", Messages: messages}}
			err := make(chan error)
			if e := msgClient.Subscribe(topics, err); e != nil {
				//log.Errorf("Failed to subscribe to edgex messagebus topic %s.\n", e)
				common.Log.Fatal(e)
			} else {
				var count int = 0
				for {
					select {
					case e1 := <-err:
						common.Log.Errorf("%s\n", e1)
						return
					case env := <-messages:
						count++
						fmt.Printf("%s\n", env.Payload)
						if count == 1 {
							return
						}
					}
				}
			}
		}
	}
}

func main() {
	if len(os.Args) == 1 {
		subEventsFromZMQ()
	} else if len(os.Args) == 3 {
		if v := os.Args[1]; v == "mqtt" {
			subEventsFromMQTT(os.Args[2])
		}
	}
}
