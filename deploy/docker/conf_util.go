package main

import (
	"fmt"
	"github.com/go-yaml/yaml"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

var khome = os.Getenv("KUIPER_HOME")

var fileMap = map[string]string{
	"edgex":       khome + "/etc/sources/edgex.yaml",
	"random":      khome + "/etc/sources/random.yaml",
	"zmq":         khome + "/etc/sources/zmq.yaml",
	"httppull":    khome + "/etc/sources/httppull.yaml",
	"mqtt_source": khome + "/etc/mqtt_source.yaml",
	"kuiper":      khome + "/etc/kuiper.yaml",
	"client":      khome + "/etc/client.yaml",
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
		"KUBEEDGEVERSION":    "kubeedgeVersion",
		"KUBEEDGEMODELFILE":  "kubeedgeModelFile",
	},
	"kuiper": {
		"CONSOLELOG":         "consoleLog",
		"FILELOG":            "fileLog",
		"RESTPORT":           "restPort",
		"RESTTLS":            "restTls",
		"PROMETHEUSPORT":     "prometheusPort",
		"PLUGINHOSTS":        "pluginHosts",
		"CHECKPOINTINTERVAL": "checkpointInterval",
		"CACHETHRESHOLD":     "cacheThreshold",
		"CACHETRIGGERCOUNT":  "cacheTriggerCount",
		"DISABLECACHE":       "disableCache",
	},
}

func main() {
	fmt.Println(fileMap["edgex"])
	files := make(map[string]map[interface{}]interface{})
	ProcessEnv(files, os.Environ())
	for f, v := range files {
		if bs, err := yaml.Marshal(v); err != nil {
			fmt.Println(err)
		} else {
			message := fmt.Sprintf("-------------------\nConf file %s: \n %s", f, printable(v))
			fmt.Println(message)
			if fname, ok := fileMap[f]; ok {
				if e := ioutil.WriteFile(fname, bs, 0644); e != nil {
					fmt.Println(e)
				} else {
					fmt.Printf("%s updated", fname)
				}
			}
		}
	}
}

func printable(m map[interface{}]interface{}) map[interface{}]interface{} {
	printableMap := make(map[interface{}]interface{})
	for k, v := range m {
		ks, ok := k.(string)
		if ok && strings.ToLower(ks) == "password" {
			printableMap[k] = "*"
		} else {
			if vm, ok := v.(map[interface{}]interface{}); ok {
				printableMap[k] = printable(vm)
			} else {
				printableMap[k] = v
			}
		}
	}
	return printableMap
}

func toPrintableString(m map[interface{}]interface{}) string {
	p := printable(m)
	b, _ := yaml.Marshal(p)
	return string(b)
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

		env_v := strings.ReplaceAll(pair[0], "__", ".")
		keys := strings.Split(env_v, ".")
		for i, v := range keys {
			keys[i] = v
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
					err = yaml.Unmarshal(data, &m)
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
		conf[key] = getValueType(val)
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

func getKey(file string, key string) string {
	if m, ok := file_keys_map[file][key]; ok {
		return m
	} else {
		return strings.ToLower(key)
	}
}

func getValueType(val string) interface{} {
	val = strings.Trim(val, " ")
	if strings.HasPrefix(val, "[") && strings.HasSuffix(val, "]") {
		val = strings.ReplaceAll(val, "[", "")
		val = strings.ReplaceAll(val, "]", "")
		vals := strings.Split(val, ",")
		var ret []interface{}
		for _, v := range vals {
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				ret = append(ret, i)
			} else if b, err := strconv.ParseBool(v); err == nil {
				ret = append(ret, b)
			} else if f, err := strconv.ParseFloat(v, 64); err == nil {
				ret = append(ret, f)
			} else {
				ret = append(ret, v)
			}
		}
		return ret
	} else if i, err := strconv.ParseInt(val, 10, 64); err == nil {
		return i
	} else if b, err := strconv.ParseBool(val); err == nil {
		return b
	} else if f, err := strconv.ParseFloat(val, 64); err == nil {
		return f
	}
	return val
}
