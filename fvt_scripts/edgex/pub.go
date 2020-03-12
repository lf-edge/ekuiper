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
	"os"
	"time"
)
var msgConfig1 = types.MessageBusConfig{
	PublishHost: types.HostInfo{
		Host:     "*",
		Port:     5570,
		Protocol: "tcp",
	},
	Type:messaging.ZeroMQ,
}

var msgConfig2 = types.MessageBusConfig{
	PublishHost: types.HostInfo{
		Host:     "*",
		Port:     5570,
		Protocol: "tcp",
	},
	Type:messaging.ZeroMQ,
}

func pubEventClientZeroMq() {
	if msgClient, err := messaging.NewMessageClient(msgConfig1); err != nil {
		log.Fatal(err)
	} else {
		if ec := msgClient.Connect(); ec != nil {
			log.Fatal(ec)
		} else {
			client := coredata.NewEventClient(local.New("test"))
			//r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for i := 0; i < 10; i++ {
				//temp := r.Intn(100)
				//humd := r.Intn(100)

				var testEvent = models.Event{Device: "demo", Created: 123, Modified: 123, Origin: 123}
				var r1 = models.Reading{Pushed: 123, Created: 123, Origin: 123, Modified: 123, Device: "test device name", Name: "Temperature", Value: fmt.Sprintf("%d", i*8)}
				var r2 = models.Reading{Pushed: 123, Created: 123, Origin: 123, Modified: 123, Device: "test device name", Name: "Humidity", Value: fmt.Sprintf("%d", i*9)}

				var r3 = models.Reading{Name:"b1"}
				if i % 2 == 0 {
					r3.Value = "true"
				} else {
					r3.Value = "false"
				}

				r4 := models.Reading{Name:"i1", Value:fmt.Sprintf("%d", i)}
				r5 := models.Reading{Name:"f1", Value:fmt.Sprintf("%.2f", float64(i)/2.0)}

				testEvent.Readings = append(testEvent.Readings, r1, r2, r3, r4, r5)

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

func pubToAnother() {
	if msgClient, err := messaging.NewMessageClient(msgConfig2); err != nil {
		log.Fatal(err)
	} else {
		if ec := msgClient.Connect(); ec != nil {
			log.Fatal(ec)
		}
		client := coredata.NewEventClient(local.New("test1"))
		var testEvent = models.Event{Device: "demo1", Created: 123, Modified: 123, Origin: 123}
		var r1 = models.Reading{Pushed: 123, Created: 123, Origin: 123, Modified: 123, Device: "test device name", Name: "Temperature", Value: "20"}
		var r2 = models.Reading{Pushed: 123, Created: 123, Origin: 123, Modified: 123, Device: "test device name", Name: "Humidity", Value: "30"}

		testEvent.Readings = append(testEvent.Readings, r1, r2)

		data, err := client.MarshalEvent(testEvent)
		if err != nil {
			fmt.Errorf("unexpected error MarshalEvent %v", err)
		} else {
			fmt.Println(string(data))
		}

		env := types.NewMessageEnvelope([]byte(data), context.Background())
		env.ContentType = "application/json"

		if e := msgClient.Publish(env, "application"); e != nil {
			log.Fatal(e)
		} else {
			fmt.Printf("pubToAnother successful: %s\n", data)
		}
	}
}

func main() {
	if len(os.Args) == 1 {
		pubEventClientZeroMq()
	} else if len(os.Args) == 2 {
		if v := os.Args[1]; v == "another" {
			pubToAnother()
		}
	}
}

