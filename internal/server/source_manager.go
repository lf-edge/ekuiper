package server

import (
	"fmt"
	"io/ioutil"

	"sigs.k8s.io/yaml"
)

type RabbitmqConfig struct {
	Url          string   `yaml:"url"`
	Exchange     string   `yaml:"exchange"`
	ExchangeType string   `yaml:"exchangeType"`
	RoutingKeys  []string `yaml:"routingKeys"`
	Username     string   `yaml:"username"`
	Password     string   `yaml:"password"`
}

func WriteSource(id string, rabbitmqConfig RabbitmqConfig) error {
	rabbitmqConfigMap := make(map[string]RabbitmqConfig)
	//read /kuiper/etc/sources/rabbit.yaml
	path := "/kuiper/etc/sources/rabbitmq.yaml"
	data, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Println("read err:", err)
	}
	yaml.Unmarshal(data, &rabbitmqConfigMap)
	fmt.Println(rabbitmqConfigMap)
	_, ok := rabbitmqConfigMap[id]
	if ok {
		fmt.Errorf("key already exist in rabbitmq config file")
	}
	rabbitmqConfigMap[id] = rabbitmqConfig
	//write /kuiper/etc/sources/rabbit.yaml
	yamlData, err := yaml.Marshal(rabbitmqConfigMap)
	if err != nil {
		fmt.Errorf("marshal rabbitmq configmap err:", err)
	}
	err = ioutil.WriteFile(path, yamlData, 755)
	if err != nil {
		fmt.Errorf("write yaml file err:", err)
	}
	return nil
}

func DeleteSource(id string) error {
	rabbitmqConfigMap := make(map[string]RabbitmqConfig)
	//read /kuiper/etc/sources/rabbit.yaml
	path := "/kuiper/etc/sources/rabbitmq.yaml"
	data, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Println("read err:", err)
	}
	yaml.Unmarshal(data, &rabbitmqConfigMap)
	delete(rabbitmqConfigMap, id)
	//write /kuiper/etc/sources/rabbit.yaml
	yamlData, err := yaml.Marshal(rabbitmqConfigMap)
	if err != nil {
		fmt.Errorf("marshal rabbitmq configmap err:", err)
	}
	err = ioutil.WriteFile(path, yamlData, 755)
	if err != nil {
		fmt.Errorf("write yaml file err:", err)
	}
	return nil
}
