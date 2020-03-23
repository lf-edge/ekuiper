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
	desc1 = "Temperature descriptor1"
	desc2 = "Humidity descriptor2"
	desc3 = "Boolean descriptor"
	desc4 = "Int descriptor"
	desc5 = "Float descriptor"
	desc6 = "String descriptor"
)

var vd1 = models.ValueDescriptor{Id: "Temperature", Created: 123, Modified: 123, Origin: 123, Name: "Temperature",
	Description: "test description", Min: -70, Max: 140, DefaultValue: 32, Formatting: "%f", Type:"Float32",
	Labels: []string{"temp", "room temp"}, UomLabel: "F", MediaType: clients.ContentTypeJSON, FloatEncoding: "eNotation"}

var vd2 = models.ValueDescriptor{Id: "Humidity", Created: 123, Modified: 123, Origin: 123, Name: "Humidity",
	Description: "test description", Min: -70, Max: 140, DefaultValue: 32, Formatting: "%d", Type:"Uint64",
	Labels: []string{"humi", "room humidity"}, UomLabel: "F", MediaType: clients.ContentTypeJSON, FloatEncoding: "eNotation"}

var vd3 = models.ValueDescriptor{Id: "b1", Name: "b1", Formatting: "%t", Type:"Bool", MediaType: clients.ContentTypeJSON}
var vd4 = models.ValueDescriptor{Id: "i1", Name: "i1", Formatting: "%d", Type:"UINT8", MediaType: clients.ContentTypeJSON}
var vd5 = models.ValueDescriptor{Id: "f1", Name: "f1", Formatting: "%f", Type:"FLOAT64", MediaType: clients.ContentTypeJSON}
var vd6 = models.ValueDescriptor{Id: "s1", Name: "s1", Formatting: "%s", Type:"String", MediaType: clients.ContentTypeJSON}

func main() {
	http.HandleFunc(clients.ApiValueDescriptorRoute, Hello)
	if e := http.ListenAndServe(":10080", nil); e != nil {
		log.Fatal(e)
	}
}

func Hello(w http.ResponseWriter, req *http.Request) {
	descriptor1 := vd1
	descriptor1.Description = desc1

	descriptor2 := vd2
	descriptor2.Description = desc2

	descriptor3 := vd3
	descriptor3.Description = desc3

	descriptor4 := vd4
	descriptor4.Description = desc4

	descriptor5 := vd5
	descriptor5.Description = desc5

	descriptor6 := vd6
	descriptor6.Description = desc6

	descriptors := []models.ValueDescriptor{descriptor1, descriptor2, descriptor3, descriptor4, descriptor5, descriptor6}

	data, err := json.Marshal(descriptors)
	if err != nil {
		fmt.Errorf("marshaling error: %s", err.Error())
	}
	if _, err := fmt.Fprintf(w, "%s", data); err != nil {
		log.Fatal(err)
	}
	//_, _ = w.Write(data)
}


