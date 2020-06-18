package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"time"
)

func alert(w http.ResponseWriter, req *http.Request) {
	buf, bodyErr := ioutil.ReadAll(req.Body)
	if bodyErr != nil {
		log.Print("bodyErr ", bodyErr.Error())
		http.Error(w, bodyErr.Error(), http.StatusInternalServerError)
		return
	}

	rdr1 := ioutil.NopCloser(bytes.NewBuffer(buf))
	log.Printf("BODY: %q", rdr1)
}

var count = 0

type Sensor struct {
	Temperature int `json: "temperature""`
	Humidity    int `json: "humidiy"`
}

var s = &Sensor{}

func pullSrv(w http.ResponseWriter, req *http.Request) {
	buf, bodyErr := ioutil.ReadAll(req.Body)
	if bodyErr != nil {
		log.Print("bodyErr ", bodyErr.Error())
		http.Error(w, bodyErr.Error(), http.StatusInternalServerError)
		return
	} else {
		fmt.Println(string(buf))
	}

	if count%2 == 0 {
		rand.Seed(time.Now().UnixNano())
		s.Temperature = rand.Intn(100)
		s.Humidity = rand.Intn(100)
	}
	fmt.Printf("%v\n", s)
	count++
	sd, err := json.Marshal(s)
	if err != nil {
		fmt.Println(err)
		return
	} else {
		if _, e := fmt.Fprintf(w, "%s", sd); e != nil {
			fmt.Println(e)
		}
	}
}

func main() {
	http.Handle("/", http.FileServer(http.Dir("web")))
	http.HandleFunc("/alert", alert)
	http.HandleFunc("/pull", pullSrv)

	http.ListenAndServe(":9090", nil)
}
