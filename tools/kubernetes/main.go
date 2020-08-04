package main

import (
	"github.com/emqx/kuiper/tools/kubernetes/util"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)
		<-sigint
		os.Exit(0)
	}()
	util.Process()
}
