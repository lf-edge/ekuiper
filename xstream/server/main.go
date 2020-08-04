package main

import "github.com/emqx/kuiper/xstream/server/server"

var (
	Version      = "unknown"
	LoadFileType = "relative"
)

func main() {
	server.StartUp(Version, LoadFileType)
}
