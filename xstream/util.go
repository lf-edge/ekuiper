package xstream

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

type Conf map[string]interface{}

var confs = make(map[string] Conf)

func GetConfAsString(file, key string) (string, error) {
	val, err := getConfValue(file, key)

	if err != nil {
		return "", err
	}

	if v, ok := val.(string); ok {
		return v, nil
	} else if val == nil {
		return "", nil
	}else {
		return "", fmt.Errorf("The value %s is not type of string for key %s.\n", val, key )
	}
}

func GetConfAsInt(file, key string) (int, error) {
	val, err := getConfValue(file, key)

	if err != nil {
		return 0, err
	}

	if v, ok := val.(float64); ok {
		return int(v), nil
	} else {
		return 0, fmt.Errorf("The value {0} is not type of int for key {1}.\n", )
	}
}

func GetConfAsFloat(file, key string) (float64, error) {
	val, err := getConfValue(file, key)

	if err != nil {
		return 0, err
	}

	if v, ok := val.(float64); ok {
		return v, nil
	} else {
		return 0, fmt.Errorf("The value {0} is not type of float for key {1}.\n", )
	}
}

func GetConfAsBool(file, key string) (bool, error) {
	val, err := getConfValue(file, key)

	if err != nil {
		return false, err
	}

	if v, ok := val.(bool); ok {
		return v, nil
	} else {
		return false, fmt.Errorf("The value {0} is not type of bool for key {1}.\n", )
	}
}


func getConfValue(file, key string) (interface{}, error) {
	if conf, ok := confs[file]; !ok {
		if c, e := initConf(file); e != nil {
			return nil, e
		} else {
			confs[file] = c
			return getValue(c, key)
		}
	} else {
		return getValue(conf, key)
	}
}

func initConf(file string) (Conf, error) {
	conf := make(Conf)
	fp, _ := filepath.Abs(file)
	if f, err1 := os.Open(fp); err1 == nil {
		defer f.Close()

		byteValue, _ := ioutil.ReadAll(f)
		if err2 := json.Unmarshal([]byte(byteValue), &conf); err2 != nil {
			return nil, err2
		}
		log.Printf("Successfully to load the configuration file %s.\n", fp)
	} else {
		//Try as absolute path
		if f, err1 := os.Open(file); err1 == nil {
			byteValue, _ := ioutil.ReadAll(f)
			if err2 := json.Unmarshal([]byte(byteValue), &conf); err2 != nil {
				return nil, err2
			}
			log.Printf("Successfully to load the configuration file %s.\n", file)
		} else {
			return nil, fmt.Errorf("Cannot load configuration file %s.\n", file)
		}
	}
	return conf, nil
}

func getValue(conf Conf, key string) (interface{}, error)  {
	keys := strings.Split(key, ".")

	if len(keys) == 1 {
		return conf[key], nil
	}

	nkey := strings.Join(keys[1:], ".")
	ckey := strings.Join(keys[0:1], "")

	if c, ok := conf[ckey].(map[string]interface {}); ok {
		return getValue(c, nkey)
	} else {

		return nil, fmt.Errorf("%s does not exsit for key %s.", conf, key)
	}
}

