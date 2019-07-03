package extensions

import (
	"context"
	"engine/common"
	"engine/xsql"
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/go-yaml/yaml"
	"github.com/google/uuid"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

var log = common.Log
type MQTTSource struct {
	srv      string
	tpc      string
	clientid string
	schema   map[string]interface{}

	outs  map[string]chan<- interface{}
	conn MQTT.Client
	name 		string
	//ctx context.Context
}


type MQTTConfig struct {
	Qos string `yaml:"qos"`
	Sharedsubscription string `yaml:"sharedsubscription"`
	Servers []string `yaml:"servers"`
	Clientid string `yaml:"clientid"`
}


const confName string = "mqtt_source.yaml"

func NewWithName(name string, topic string, confKey string) (*MQTTSource, error) {
	confDir, err := common.GetConfLoc()
	var file string = confDir + confName
	if err != nil {
		//Try the development mode, read from workspace
		file = "xstream/extensions/" + confName
		if abs, err1 := filepath.Abs(file); err1 != nil {
			return nil, err1
		} else {
			file = abs
		}
	}

	b, err := ioutil.ReadFile(file)
	if err != nil {
		log.Fatal(err)
	}

	var cfg map[string]MQTTConfig
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		log.Fatal(err)
	}

	ms := &MQTTSource{tpc: topic, name: name}
	ms.outs = make(map[string]chan<- interface{})
	if srvs := cfg[confKey].Servers; srvs != nil && len(srvs) > 1 {
		return nil, fmt.Errorf("It only support one server in %s section.", confKey)
	} else if srvs == nil {
		srvs = cfg["default"].Servers
		if srvs != nil && len(srvs) == 1 {
			ms.srv = srvs[0]
		} else {
			return nil, fmt.Errorf("Wrong configuration in default section!")
		}
	} else {
		ms.srv = srvs[0]
	}

	if cid := cfg[confKey].Clientid; cid != "" {
		ms.clientid = cid
	} else {
		ms.clientid = cfg["default"].Clientid
	}
	return ms, nil
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func (ms *MQTTSource) WithSchema(schema string) *MQTTSource {
	return ms
}

func (ms *MQTTSource) GetName() string {
	return ms.name
}

func (ms *MQTTSource) AddOutput(output chan<- interface{}, name string) {
	if _, ok := ms.outs[name]; !ok{
		ms.outs[name] = output
	}else{
		log.Error("fail to add output %s, operator %s already has an output of the same name", name, ms.name)
	}
}

func (ms *MQTTSource) Open(ctx context.Context) error {
	go func() {
		exeCtx, cancel := context.WithCancel(ctx)
		opts := MQTT.NewClientOptions().AddBroker(ms.srv)

		if ms.clientid == "" {
			if uuid, err := uuid.NewUUID(); err != nil {
				log.Printf("Failed to get uuid, the error is %s.\n", err)
				cancel()
				return
			} else {
				opts.SetClientID(uuid.String())
			}
		} else {
			opts.SetClientID(ms.clientid)
		}

		h := func(client MQTT.Client, msg MQTT.Message) {
			if ms.tpc != msg.Topic() {
				select {
				case <-exeCtx.Done():
					log.Println("Done 1.")
					ms.conn.Disconnect(5000)
				}
				return
			} else {
				log.Infof("received %s", msg.Payload())
				tuple := &xsql.Tuple{EmitterName:ms.name, Message:msg.Payload(), Timestamp: common.TimeToUnixMilli(time.Now())}
				for _, out := range ms.outs{
					out <- tuple
				}
			}
		}

		opts.SetDefaultPublishHandler(h)
		c := MQTT.NewClient(opts)
		if token := c.Connect(); token.Wait() && token.Error() != nil {
			log.Fatalf("Found error: %s.\n", token.Error())
			cancel()
		}
		log.Printf("The connection to server %s was established successfully.\n", ms.srv)
		ms.conn = c
		if token := c.Subscribe(ms.tpc, 0, nil); token.Wait() && token.Error() != nil {
			log.Fatalf("Found error: %s.\n", token.Error())
			cancel()
		}
		log.Printf("Successfully subscribe to topic %s.\n", ms.tpc)
	}()

	return nil
}
