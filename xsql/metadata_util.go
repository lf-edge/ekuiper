package xsql

import "strings"

const INTERNAL_MQTT_TOPIC_KEY string = "internal_mqtt_topic_key_$$"
const INTERNAL_MQTT_MSG_ID_KEY string = "internal_mqtt_msg_id_key_$$"

//For functions such as mqtt(topic). If the field definitions also has a field named "topic", then it need to
//have an internal key for "topic" to avoid key conflicts.
var SpecialKeyMapper = map[string]string{"topic" : INTERNAL_MQTT_TOPIC_KEY, "messageid" : INTERNAL_MQTT_MSG_ID_KEY}
func AddSpecialKeyMap(left, right string) {
	SpecialKeyMapper[left] = right
}

/**
The function is used for re-write the parameter names.
For example, for mqtt function, the arguments could be 'topic' or 'messageid'.
If the field name defined in stream happens to be 'topic' or 'messageid', it will have conflicts.
 */
func (c Call) rewrite_func() *Call {
	if strings.ToLower(c.Name) == "mqtt" {
		if f, ok := c.Args[0].(*FieldRef); ok {
			if n, ok1 := SpecialKeyMapper[f.Name]; ok1 {
				f.Name = n
				c.Args[0] = f
			}
		}
	}
	return &c
}