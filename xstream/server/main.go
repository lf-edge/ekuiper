package main

import "github.com/emqx/kuiper/xstream/server/server"

var Version string = "unknown"

func main() {
	server.StartUp(Version)
}
