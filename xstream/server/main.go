package main

import "github.com/emqx/kuiper/xstream/server/server"

var Version = "unknown"

func main() {
	server.StartUp(Version)
}
