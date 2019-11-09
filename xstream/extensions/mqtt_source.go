package extensions

import (
	"context"
	"encoding/json"
	"engine/common"
	"engine/xsql"
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/go-yaml/yaml"
	"github.com/google/uuid"
	"os"
	"strconv"
	"strings"
	"time"
)

type MQTTSource struct {
	srv      string
	tpc      string
	clientid string
	pVersion uint
	uName 	 string
	password string

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
	PVersion string `yaml:"protocolVersion"`
	Uname string `yaml:"username"`
	Password string `yaml:"password"`
}

const confName string = "mqtt_source.yaml"

func NewWithName(name string, topic string, confKey string) (*MQTTSource, error) {
	b := common.LoadConf(confName)
	var cfg map[string]MQTTConfig
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, err
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

	var pversion uint = 3
	if pv := cfg[confKey].PVersion; pv != "" {
		if pv == "3.1.1" {
			pversion = 4
		}
	}
	ms.pVersion = pversion

	if uname := cfg[confKey].Uname; uname != "" {
		ms.uName = strings.Trim(uname, " ")
	}

	if password := cfg[confKey].Password; password != "" {
		ms.password = strings.Trim(password, " ")
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
		common.Log.Warnf("fail to add output %s, operator %s already has an output of the same name", name, ms.name)
	}
}

func (ms *MQTTSource) Open(ctx context.Context) error {
	log := common.GetLogger(ctx)
	go func() {
		exeCtx, cancel := context.WithCancel(ctx)
		opts := MQTT.NewClientOptions().AddBroker(ms.srv).SetProtocolVersion(ms.pVersion)

		if ms.clientid == "" {
			if uuid, err := uuid.NewUUID(); err != nil {
				log.Printf("Failed to get uuid, the error is %s", err)
				cancel()
				return
			} else {
				opts.SetClientID(uuid.String())
			}
		} else {
			opts.SetClientID(ms.clientid)
		}

		if ms.uName != "" {
			opts.SetUsername(ms.uName)
		}

		if ms.password != "" {
			opts.SetPassword(ms.password)
		}


		h := func(client MQTT.Client, msg MQTT.Message) {
			log.Infof("received %s", msg.Payload())

			result := make(map[string]interface{})
			//The unmarshal type can only be bool, float64, string, []interface{}, map[string]interface{}, nil
			if e := json.Unmarshal(msg.Payload(), &result); e != nil {
				log.Errorf("Invalid data format, cannot convert %s into JSON with error %s", string(msg.Payload()), e)
				return
			}
			//Convert the keys to lowercase
			result = xsql.LowercaseKeyMap(result)
			result[xsql.INTERNAL_MQTT_TOPIC_KEY] = msg.Topic()
			result[xsql.INTERNAL_MQTT_MSG_ID_KEY] = strconv.Itoa(int(msg.MessageID()))

			tuple := &xsql.Tuple{Emitter: ms.tpc, Message:result, Timestamp: common.TimeToUnixMilli(time.Now())}
			for _, out := range ms.outs{
				out <- tuple
			}
		}

		opts.SetDefaultPublishHandler(h)
		c := MQTT.NewClient(opts)
		if token := c.Connect(); token.Wait() && token.Error() != nil {
			log.Printf("Found error when connecting to %s for %s: %s", ms.srv, ms.name, token.Error())
			cancel()
			return
		}
		log.Printf("The connection to server %s was established successfully", ms.srv)
		ms.conn = c
		if token := c.Subscribe(ms.tpc, 0, nil); token.Wait() && token.Error() != nil {
			log.Printf("Found error: %s", token.Error())
			cancel()
			return
		}
		log.Printf("Successfully subscribe to topic %s", ms.tpc)
		select {
		case <-exeCtx.Done():
			log.Println("Mqtt Source Done")
			ms.conn.Disconnect(5000)
			cancel()
		}
	}()

	return nil
}
