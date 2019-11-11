package xsql

const INTERNAL_MQTT_TOPIC_KEY string = "topic"
const INTERNAL_MQTT_MSG_ID_KEY string = "id"

var SpecialKeyMapper = map[string]string{INTERNAL_MQTT_TOPIC_KEY : "", INTERNAL_MQTT_MSG_ID_KEY : ""}

func AddSpecialKeyMap(key string) {
	SpecialKeyMapper[key] = ""
}
