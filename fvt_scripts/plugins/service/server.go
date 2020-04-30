package main

import (
	"bytes"
	"io/ioutil"
	"log"
	"net/http"
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

func main() {
	http.Handle("/", http.FileServer(http.Dir("web")))
	http.HandleFunc("/alert", alert)

	http.ListenAndServe(":9090", nil)
}
