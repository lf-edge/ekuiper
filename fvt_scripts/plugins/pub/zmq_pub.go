package main

import (
	"encoding/json"
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"os"
	"time"
)

type zmqPub struct {
	publisher *zmq.Socket
	srv       string
	topic     string
}

func (m *zmqPub) Open() (err error) {
	m.publisher, err = zmq.NewSocket(zmq.PUB)
	if err != nil {
		return fmt.Errorf("zmq sink fails to create socket: %v", err)
	}
	err = m.publisher.Bind(m.srv)
	if err != nil {
		return fmt.Errorf("zmq sink fails to bind to %s: %v", m.srv, err)
	}
	fmt.Println("zmq sink open")
	return nil
}

func (m *zmqPub) Send(item interface{}) (err error) {
	if v, ok := item.([]byte); ok {
		fmt.Printf("To pub: %s \n", item)
		if m.topic == "" {
			_, err = m.publisher.Send(string(v), 0)
		} else {
			msgs := []string{
				m.topic,
				string(v),
			}
			_, err = m.publisher.SendMessage(msgs)
		}
	} else {
		fmt.Printf("zmq sink receive non byte data %v \n", item)
	}
	if err != nil {
		fmt.Printf("send to zmq error %v \n", err)
	}
	return
}

func (m *zmqPub) Close() error {
	if m.publisher != nil {
		return m.publisher.Close()
	}
	return nil
}

type data struct {
	Temperature int `json:"temperature"`
	Humidity    int `json:"humidity"`
}

var mockup = [10]data{
	{Temperature: 10, Humidity: 15},
	{Temperature: 15, Humidity: 20},
	{Temperature: 20, Humidity: 25},
	{Temperature: 25, Humidity: 30},
	{Temperature: 30, Humidity: 35},
	{Temperature: 35, Humidity: 40},
	{Temperature: 40, Humidity: 45},
	{Temperature: 45, Humidity: 50},
	{Temperature: 50, Humidity: 55},
	{Temperature: 55, Humidity: 60},
}

func main() {
	zmq := zmqPub{srv: "tcp://127.0.0.1:5563", topic: "events"}
	if e := zmq.Open(); e != nil {
		return
	} else {
		if len(os.Args) == 2 {
			v := os.Args[1]
			if v != "" {
				zmq.topic = v
				fmt.Printf("Use the topic %s\n", v)
			} else {
				fmt.Printf("Use the default zeromq topic %s\n", "events")
			}
		}

		for i := 0; i < 20; i++ {
			index := i % 10
			b, _ := json.Marshal(mockup[index])
			time.Sleep(1000 * time.Millisecond)
			zmq.Send(b)
		}
	}
}

