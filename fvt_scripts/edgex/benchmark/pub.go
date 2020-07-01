// +build benchmark

//Not necessary to build the file, until for the edgex benchmark test
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
	"strconv"
	"sync"
	"time"
)

var msgConfig1 = types.MessageBusConfig{
	PublishHost: types.HostInfo{
		Host:     "172.31.1.144",
		Port:     5563,
		Protocol: "tcp",
	},
	Type: messaging.ZeroMQ,
}

type data struct {
	temperature int
	humidity    int
}

var mockup = []data{
	{temperature: 10, humidity: 15},
	{temperature: 15, humidity: 20},
	{temperature: 20, humidity: 25},
	{temperature: 25, humidity: 30},
	{temperature: 30, humidity: 35},
	{temperature: 35, humidity: 40},
	{temperature: 40, humidity: 45},
	{temperature: 45, humidity: 50},
	{temperature: 50, humidity: 55},
	{temperature: 55, humidity: 60},
}

func pubEventClientZeroMq(count int, wg *sync.WaitGroup) {
	defer wg.Done()
	if msgClient, err := messaging.NewMessageClient(msgConfig1); err != nil {
		log.Fatal(err)
	} else {
		if ec := msgClient.Connect(); ec != nil {
			log.Fatal(ec)
		} else {
			client := coredata.NewEventClient(local.New("test"))
			index := 0
			for i := 0; i < count; i++ {
				if i%10 == 0 {
					index = 0
				}

				var testEvent = models.Event{Device: "demo"}
				var r1 = models.Reading{Device: "Temperature device", Name: "Temperature", Value: fmt.Sprintf("%d", mockup[index].temperature)}
				var r2 = models.Reading{Device: "Humidity device", Name: "Humidity", Value: fmt.Sprintf("%d", mockup[index].humidity)}
				index++

				testEvent.Readings = append(testEvent.Readings, r1, r2)

				data, err := client.MarshalEvent(testEvent)
				if err != nil {
					fmt.Errorf("unexpected error MarshalEvent %v", err)
				}

				env := types.NewMessageEnvelope([]byte(data), context.Background())
				env.ContentType = "application/json"

				if e := msgClient.Publish(env, "events"); e != nil {
					log.Fatal(e)
				} else {
					//fmt.Printf("%d - %s\n", index, string(data))
				}
				time.Sleep(100 * time.Nanosecond)
			}
		}
	}
}

func main() {
	start := time.Now()
	count := 1000
	if len(os.Args) == 2 {
		v := os.Args[1]
		if c, err := strconv.Atoi(v); err != nil {
			fmt.Errorf("%s\n", err)
		} else {
			count = c
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go pubEventClientZeroMq(count, &wg)
	}
	wg.Wait()
	t := time.Now()
	elapsed := t.Sub(start)

	fmt.Printf("elapsed %2fs\n", elapsed.Seconds())
}
