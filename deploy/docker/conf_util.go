package main

import (
	"fmt"
	"github.com/go-yaml/yaml"
	"io/ioutil"
	"os"
	"strings"
)

var fileMap = map[string]string{
	//"edgex":       "/kuiper/etc/sources/edgex.yaml",
	"edgex":       "/tmp/edgex.yaml",
	"mqtt_source": "/kuiper/etc/mqtt_source.yaml",
	"kuiper":      "/kuiper/etc/kuiper.yaml",
}

var file_keys_map = map[string]map[string]string{
	"edgex": {
		"CLIENTID":          "ClientId",
		"USERNAME":          "Username",
		"PASSWORD":          "Password",
		"QOS":               "Qos",
		"KEEPALIVE":         "KeepAlive",
		"RETAINED":          "Retained",
		"CONNECTIONPAYLOAD": "ConnectionPayload",
		"CERTFILE":          "CertFile",
		"KEYFILE":           "KeyFile",
		"CERTPEMBLOCK":      "CertPEMBlock",
		"KEYPEMBLOCK":       "KeyPEMBlock",
		"SKIPCERTVERIFY":    "SkipCertVerify",
	},
	"mqtt_source": {
		"SHAREDSUBSCRIPTION": "sharedSubscription",
		"CERTIFICATIONPATH":  "certificationPath",
		"PRIVATEKEYPATH":     "privateKeyPath",
	},
	"kuiper": {
		"CONSOLELOG":     "consoleLog",
		"FILELOG":        "fileLog",
		"RESTPORT":       "restPort",
		"PROMETHEUSPORT": "prometheusPort",
	},
}

func main() {
	files := make(map[string]map[interface{}]interface{})
	ProcessEnv(files, os.Environ())
	for f, v := range files {
		if bs, err := yaml.Marshal(v); err != nil {
			fmt.Println(err)
		} else {
			message := fmt.Sprintf("-------------------\nConf file %s: \n %s", f, string(bs))
			fmt.Println(message)
		}
	}
}

func ProcessEnv(files map[string]map[interface{}]interface{}, vars []string) {
	for _, e := range vars {
		pair := strings.SplitN(e, "=", 2)
		if len(pair) != 2 {
			fmt.Printf("invalid env %s, skip it.\n", e)
			continue
		}

		valid := false
		for k, _ := range fileMap {
			if strings.HasPrefix(pair[0], strings.ToUpper(k)) {
				valid = true
				break
			}
		}
		if !valid {
			continue
		} else {
			fmt.Printf("Find env: %s, start to handle it.\n", e)
		}

		env_v := strings.ReplaceAll(pair[0], "__", "+")
		keys := strings.Split(env_v, "_")
		for i, v := range keys {
			keys[i] = strings.ReplaceAll(v, "+", "_")
		}

		if len(keys) < 2 {
			fmt.Printf("not concerned env %s, skip it.\n", e)
			continue
		} else {
			k := strings.ToLower(keys[0])
			if v, ok := files[k]; !ok {
				if data, err := ioutil.ReadFile(fileMap[k]); err != nil {
					fmt.Printf("%s\n", err)
				} else {
					m := make(map[interface{}]interface{})
					err = yaml.Unmarshal([]byte(data), &m)
					if err != nil {
						fmt.Println(err)
					}
					files[k] = m
					Handle(k, m, keys[1:], pair[1])

				}
			} else {
				Handle(k, v, keys[1:], pair[1])
			}
		}
	}
}

func Handle(file string, conf map[interface{}]interface{}, skeys []string, val string) {
	key := getKey(file, skeys[0])
	if len(skeys) == 1 {
		conf[key] = val
	} else if len(skeys) >= 2 {
		if v, ok := conf[key]; ok {
			if v1, ok1 := v.(map[interface{}]interface{}); ok1 {
				Handle(file, v1, skeys[1:], val)
			} else {
				fmt.Printf("Not expected map: %v\n", v)
			}
		} else {
			v1 := make(map[interface{}]interface{})
			conf[key] = v1
			Handle(file, v1, skeys[1:], val)
		}
	}
}

func getKey(file string, key string) string{
	if m, ok := file_keys_map[file][key]; ok {
		return m
	} else {
		return strings.ToLower(key)
	}
}
