package rabbitmq

type RabbitMQRoutingKey string

const (
	RabbitMQRoutingKeyDeviceModel         RabbitMQRoutingKey = "device-model"
	RabbitMQRoutingKeyDevice              RabbitMQRoutingKey = "device"
	RabbitMQRoutingKeySceneLink           RabbitMQRoutingKey = "scene-link"
	RabbitMQRoutingKeyDeviceData          RabbitMQRoutingKey = "device-data"
	RabbitMQRoutingKeyDeviceAlarm         RabbitMQRoutingKey = "device-alarm"
	RabbitMQRoutingKeyDeviceStatus        RabbitMQRoutingKey = "device-status"
	RabbitMQRoutingKeyDeviceCommandStatus RabbitMQRoutingKey = "device-command-status"
	RabbitMQRoutingKeyServerSub           RabbitMQRoutingKey = "server-subscription"
)

type RoutingKey string

const (
	RoutingKeyDeviceProperty RoutingKey = "property"
	RoutingKeyDeviceAlarm    RoutingKey = "alarm"
	RoutingKeyDeviceStatus   RoutingKey = "status"
	RoutingKeyDeviceCommand  RoutingKey = "command"
)

type RabbitMQEvent string

const (
	RabbitMQEventAdd    RabbitMQEvent = "add"
	RabbitMQEventUpdate RabbitMQEvent = "update"
	RabbitMQEventDelete RabbitMQEvent = "delete"
)

type BaseUnion struct {
	Name string `json:"name,omitempty"`
	Id   string `json:"id,omitempty"`
}

// type RabbitMQBody struct {
// 	MsgId     string             `json:"msgId,omitempty"`
// 	MsgTopic  RabbitMQRoutingKey `json:"msgTopic,omitempty"`
// 	MsgEvent  RabbitMQEvent      `json:"msgEvent,omitempty"`
// 	Timestamp string             `json:"timestamp,omitempty"`
// 	Body      []byte             `json:"body,omitempty"`
// }

type RabbitMQMsg struct {
	MsgId     string  `json:"msgId,omitempty"`
	CommandId string  `json:"commandId,omitempty"`
	Version   string  `json:"version,omitempty"`
	Topic     string  `json:"topic,omitempty"`
	Code      int     `json:"code,omitempty"`
	Message   string  `json:"message,omitempty"`
	Timestamp string  `json:"timestamp,omitempty"`
	Params    []Param `json:"params,omitempty"`
}

type Param struct {
	Id        string `json:"id,omitempty"`
	Value     string `json:"value,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
}

type DeviceData struct {
	Device      BaseUnion   `json:"device,omitempty"`
	DeviceModel BaseUnion   `json:"deviceModel,omitempty"`
	Properties  []Property  `json:"properties,omitempty"`
	Alarms      []BaseUnion `json:"alarms,omitempty"`
	Status      string      `json:"status,omitempty"`
}

type Property struct {
	BaseUnion
	ValueType   string `json:"valueType,omitempty"`
	ReportValue string `json:"reportValue,omitempty"`
	DesireValue string `json:"desireValue,omitempty"`
}
