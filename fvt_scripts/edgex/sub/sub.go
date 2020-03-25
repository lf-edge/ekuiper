package main

import (
	"fmt"
	"github.com/edgexfoundry/go-mod-messaging/messaging"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"github.com/emqx/kuiper/common"
)

func main() {
	var msgConfig1 = types.MessageBusConfig{
		SubscribeHost: types.HostInfo{
			Host:     "localhost",
			Port:     5563,
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
			if err := msgClient.Connect(); err != nil {
				common.Log.Fatal(err)
			}
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
