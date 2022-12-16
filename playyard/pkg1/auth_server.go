package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

func hello(w http.ResponseWriter, req *http.Request) {

	fmt.Fprintf(w, "hello\n")
}

func headers(w http.ResponseWriter, req *http.Request) {

	for name, headers := range req.Header {
		for _, h := range headers {
			fmt.Fprintf(w, "%v: %v\n", name, h)
		}
	}
}

func auth(w http.ResponseWriter, req *http.Request) {
	reqBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		fmt.Errorf("%s", err)
		return
	}
	var x map[string]interface{}
	json.Unmarshal([]byte(reqBody), &x)
	u,_ := x["username"]
	p,_ := x["password"]
	fmt.Printf("username and password: %s, %s\n", u, p)
	if u == "user1" && p == "user1" {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		content := []byte(`{"result": "allow", "is_superuser": false}`)
		w.Write(content)
		fmt.Println("OK")
	} else {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Println("bad")
	}
	//break
	//switch req.Method {
	//case "POST":
	//	if reqBody, err := ioutil.ReadAll(req.Body); err != nil {
	//		fmt.Errorf("%s", err)
	//		break
	//	} else {
	//
	//		//fmt.Println(x)
	//		//fmt.Printf("%s\n", reqBody)
	//	}
	//
	//default:
	//	w.WriteHeader(http.StatusNotImplemented)
	//	w.Write([]byte(http.StatusText(http.StatusNotImplemented)))
	//}
}

func data(w http.ResponseWriter, req *http.Request) {
	reqBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		fmt.Errorf("%s", err)
		return
	}
	var x map[string]interface{}
	json.Unmarshal([]byte(reqBody), &x)
	fmt.Printf("%v", x)
}

func main() {

	http.HandleFunc("/hello", hello)
	http.HandleFunc("/headers", headers)
	http.HandleFunc("/auth", auth)
	http.HandleFunc("/data", data)
	http.ListenAndServe(":8090", nil)
}
