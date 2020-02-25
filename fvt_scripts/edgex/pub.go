package main

import (
	"context"
	"fmt"
	"github.com/edgexfoundry/go-mod-core-contracts/clients/coredata"
	"github.com/edgexfoundry/go-mod-core-contracts/clients/urlclient/local"
	"github.com/edgexfoundry/go-mod-core-contracts/models"
	"github.com/edgexfoundry/go-mod-messaging/messaging"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"log"
	"math/rand"
	"time"
)
var msgConfig1 = types.MessageBusConfig{
	PublishHost: types.HostInfo{
		Host:     "*",
		Port:     5570,
		Protocol: "tcp",
	},
}

func pubEventClientZeroMq() {
	msgConfig1.Type = messaging.ZeroMQ
	if msgClient, err := messaging.NewMessageClient(msgConfig1); err != nil {
		log.Fatal(err)
	} else {
		if ec := msgClient.Connect(); ec != nil {
			log.Fatal(ec)
		} else {
			client := coredata.NewEventClient(local.New("test"))
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for i := 0; i < 10; i++ {
				temp := r.Intn(100)
				humd := r.Intn(100)

				var testEvent = models.Event{Device: "demo", Created: 123, Modified: 123, Origin: 123}
				var testReading1 = models.Reading{Pushed: 123, Created: 123, Origin: 123, Modified: 123, Device: "test device name",
					Name: "Temperature", Value: fmt.Sprintf("%d", temp)}
				var testReading2 = models.Reading{Pushed: 123, Created: 123, Origin: 123, Modified: 123, Device: "test device name",
					Name: "Humidity", Value: fmt.Sprintf("%d", humd)}
				testEvent.Readings = append(testEvent.Readings, testReading1, testReading2)

				data, err := client.MarshalEvent(testEvent)
				if err != nil {
					fmt.Errorf("unexpected error MarshalEvent %v", err)
				} else {
					fmt.Println(string(data))
				}

				env := types.NewMessageEnvelope([]byte(data), context.Background())
				env.ContentType = "application/json"

				if e := msgClient.Publish(env, "events"); e != nil {
					log.Fatal(e)
				} else {
					fmt.Printf("Pub successful: %s\n", data)
				}
				time.Sleep(1 * time.Second)
			}


		}
	}
}

func main() {
	pubEventClientZeroMq()
}

