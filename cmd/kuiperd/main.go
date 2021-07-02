package main

import "github.com/lf-edge/ekuiper/internal/server"

var (
	Version      = "unknown"
	LoadFileType = "relative"
)

func main() {
	server.StartUp(Version, LoadFileType)
}
