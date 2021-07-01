package main

import "github.com/emqx/kuiper/internal/server"

var (
	Version      = "unknown"
	LoadFileType = "relative"
)

func main() {
	server.StartUp(Version, LoadFileType)
}
