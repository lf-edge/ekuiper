package catalog

type StreamType int

const (
	MockType StreamType = iota
	MqttType
)

type Stream struct {
	StreamName string
	StreamType StreamType
	MqttStream *MqttStream
}

type MqttStream struct {
	Config *MqttStreamConfig
}

type MqttStreamConfig struct {
}
