package extensions

import (
	"crypto/tls"
	"encoding/json"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"fmt"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"strconv"
	"strings"
)

type MQTTSource struct {
	srv      string
	tpc      string
	clientid string
	pVersion uint
	uName 	 string
	password string
	certPath string
	pkeyPath string

	schema   map[string]interface{}
	conn MQTT.Client
}


type MQTTConfig struct {
	Qos int `json:"qos"`
	Sharedsubscription bool `json:"sharedSubscription"`
	Servers []string `json:"servers"`
	Clientid string `json:"clientid"`
	PVersion string `json:"protocolVersion"`
	Uname string `json:"username"`
	Password string `json:"password"`
	Certification string `json:"certificationPath"`
	PrivateKPath string `json:"privateKeyPath"`
}

func (ms *MQTTSource) WithSchema(schema string) *MQTTSource {
	return ms
}

func (ms *MQTTSource) Configure(topic string, props map[string]interface{}) error {
	cfg := &MQTTConfig{}
	err := common.MapToStruct(props, cfg)
	if err != nil {
		return fmt.Errorf("read properties %v fail with error: %v", props, err)
	}
	ms.tpc = topic
	if srvs := cfg.Servers; srvs != nil && len(srvs) > 0 {
		ms.srv = srvs[0]
	} else {
		return fmt.Errorf("missing server property")
	}

	ms.clientid = cfg.Clientid

	ms.pVersion = 3
	if cfg.PVersion == "3.1.1" {
		ms.pVersion = 4
	}

	ms.uName = cfg.Uname
	ms.password = strings.Trim(cfg.PVersion, " ")
	ms.certPath = cfg.Certification
	ms.pkeyPath = cfg.PrivateKPath
	return nil
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

	if ms.certPath != "" || ms.pkeyPath != "" {
		log.Infof("Connect MQTT broker with certification and keys.")
		if cp, err := common.ProcessPath(ms.certPath); err == nil {
			log.Infof("The certification file is %s.", cp)
			if kp, err1 := common.ProcessPath(ms.pkeyPath); err1 == nil {
				log.Infof("The private key file is %s.", kp)
				if cer, err2 := tls.LoadX509KeyPair(cp, kp); err2 != nil {
					return err2
				} else {
					opts.SetTLSConfig(&tls.Config{Certificates: []tls.Certificate{cer}})
				}
			} else {
				return err1
			}
		} else {
			return err
		}
	} else {
		log.Infof("Connect MQTT broker with username and password.")
		if ms.uName != "" {
			opts = opts.SetUsername(ms.uName)
		}

		if ms.password != "" {
			opts = opts.SetPassword(ms.password)
		}
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
		consume(result, meta)
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
