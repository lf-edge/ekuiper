package main

import (
	"encoding/json"
	"fmt"
	"github.com/edgexfoundry/go-mod-core-contracts/clients"
	"github.com/edgexfoundry/go-mod-core-contracts/models"
	"log"
	"net/http"
)

const (
	testValueDesciptorDescription1 = "Temperature descriptor1"
	testValueDesciptorDescription2 = "Humidity descriptor2"
)

var testValueDescriptor1 = models.ValueDescriptor{Id: "Temperature", Created: 123, Modified: 123, Origin: 123, Name: "Temperature",
	Description: "test description", Min: -70, Max: 140, DefaultValue: 32, Formatting: "%d", Type:"STRING",
	Labels: []string{"temp", "room temp"}, UomLabel: "F", MediaType: clients.ContentTypeJSON, FloatEncoding: "eNotation"}

var testValueDescriptor2 = models.ValueDescriptor{Id: "Humidity", Created: 123, Modified: 123, Origin: 123, Name: "Humidity",
	Description: "test description", Min: -70, Max: 140, DefaultValue: 32, Formatting: "%d", Type:"INT",
	Labels: []string{"humi", "room humidity"}, UomLabel: "F", MediaType: clients.ContentTypeJSON, FloatEncoding: "eNotation"}

func main() {
	http.HandleFunc(clients.ApiValueDescriptorRoute, Hello)
	if e := http.ListenAndServe(":10080", nil); e != nil {
		log.Fatal(e)
	}
}

func Hello(w http.ResponseWriter, req *http.Request) {
	descriptor1 := testValueDescriptor1
	descriptor1.Description = testValueDesciptorDescription1

	descriptor2 := testValueDescriptor2
	descriptor2.Description = testValueDesciptorDescription2
	descriptors := []models.ValueDescriptor{descriptor1, descriptor2}

	data, err := json.Marshal(descriptors)
	if err != nil {
		fmt.Errorf("marshaling error: %s", err.Error())
	}
	if _, err := fmt.Fprintf(w, "%s", data); err != nil {
		log.Fatal(err)
	}
	//_, _ = w.Write(data)
}


