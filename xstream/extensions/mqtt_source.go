package extensions

import (
	"encoding/json"
	"engine/common"
	"engine/xsql"
	"engine/xstream/api"
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/go-yaml/yaml"
	"github.com/google/uuid"
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
	conn MQTT.Client
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

func NewMQTTSource(topic string, confKey string) (*MQTTSource, error) {
	b := common.LoadConf(confName)
	var cfg map[string]MQTTConfig
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, err
	}

	ms := &MQTTSource{tpc: topic}
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

func (ms *MQTTSource) WithSchema(schema string) *MQTTSource {
	return ms
}

func (ms *MQTTSource) Open(ctx api.StreamContext, consume api.ConsumeFunc) error {
	log := ctx.GetLogger()

	opts := MQTT.NewClientOptions().AddBroker(ms.srv).SetProtocolVersion(ms.pVersion)
	if ms.clientid == "" {
		if uuid, err := uuid.NewUUID(); err != nil {
			return fmt.Errorf("failed to get uuid, the error is %s", err)
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

		meta := make(map[string]interface{})
		meta[xsql.INTERNAL_MQTT_TOPIC_KEY] = msg.Topic()
		meta[xsql.INTERNAL_MQTT_MSG_ID_KEY] = strconv.Itoa(int(msg.MessageID()))

		tuple := &xsql.Tuple{Emitter: ms.tpc, Message:result, Timestamp: common.TimeToUnixMilli(time.Now()), Metadata:meta}
		consume(tuple)
	}
	//TODO error listener?
	opts.SetDefaultPublishHandler(h)
	c := MQTT.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("found error when connecting to %s: %s", ms.srv, token.Error())
	}
	log.Printf("The connection to server %s was established successfully", ms.srv)
	ms.conn = c
	if token := c.Subscribe(ms.tpc, 0, nil); token.Wait() && token.Error() != nil {
		return fmt.Errorf("Found error: %s", token.Error())
	}
	log.Printf("Successfully subscribe to topic %s", ms.tpc)

	return nil
}

func (ms *MQTTSource) Close(ctx api.StreamContext) error{
	ctx.GetLogger().Println("Mqtt Source Done")
	if ms.conn != nil && ms.conn.IsConnected() {
		ms.conn.Disconnect(5000)
	}
	return nil
}
