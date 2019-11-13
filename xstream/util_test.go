package xstream

import (
	"testing"
)

func TestConf(t *testing.T) {
	var file = "test/testconf.json"

	if v, e := GetConfAsString(file, "conf_string"); e != nil || (v != "test") {
		t.Errorf("Expect %s, actual %s; error is %s. \n", "test", v, e)
	}

	if v, e := GetConfAsInt(file, "conf_int"); e != nil || (v != 10) {
		t.Errorf("Expect %s, actual %d. error is %s. \n ", "10", v, e)
	}

	if v, e := GetConfAsFloat(file, "conf_float"); e != nil || (v != 32.3) {
		t.Errorf("Expect %s, actual %f. error is %s. \n ", "32.3", v, e)
	}

	if v, e := GetConfAsBool(file, "conf_bool"); e != nil || (v != true) {
		t.Errorf("Expect %s, actual %v. error is %s. \n", "true", v, e)
	}

	if v, e := GetConfAsString(file, "servers.srv1.addr"); e != nil || (v != "127.0.0.1") {
		t.Errorf("Expect %s, actual %s. error is %s. \n", "127.0.0.1", v, e)
	}

	if v, e := GetConfAsString(file, "servers.srv1.clientid"); e != nil || (v != "") {
		t.Errorf("Expect %s, actual %s. error is %s. \n", "", v, e)
	}

	if v, e := GetConfAsInt(file, "servers.srv2.port"); e != nil || (v != 1883) {
		t.Errorf("Expect %s, actual %d. error is %s. \n", "1883", v, e)
	}

}
