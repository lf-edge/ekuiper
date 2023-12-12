// Copyright 2021-2023 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"embed"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/canvas"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/server"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

//go:embed icon.png
var icon []byte

//go:embed banner.png
var banner []byte

//go:embed etc
var etc embed.FS

var (
	Version       = "unknown"
	LoadFileType  = "absolute"
	baseDirectory = ""
)

var lblMsg *widget.Label
var btnQuit *widget.Button
var btnInit *widget.Button
var btnStart *widget.Button
var application fyne.App

func setDirectory() {
	conf.PathConfig.LoadFileType = LoadFileType
	conf.PathConfig.Dirs["etc"] = baseDirectory + "etc"
	conf.PathConfig.Dirs["data"] = baseDirectory + "data"
	conf.PathConfig.Dirs["log"] = baseDirectory + "log"
	conf.PathConfig.Dirs["plugins"] = baseDirectory + "plugins"
}

func initService() {
	neededDirs := []string{
		"data", "log",
		"etc", "etc/services", "etc/services/schemas", "etc/services/schemas/google", "etc/services/schemas/google/api", "etc/sources", "etc/connections", "etc/mgmt", "etc/ops", "etc/sinks", "etc/multilingual",
		"plugins", "plugins/sources", "plugins/portable", "plugins/wasm", "plugins/functions", "plugins/sinks",
	}

	mkAllDirSucceed := true
	for _, dir := range neededDirs {
		err := os.MkdirAll(baseDirectory+dir, os.ModePerm)
		if err != nil {
			displayErrorMessage(err.Error())
			mkAllDirSucceed = false
			break
		}
	}

	if mkAllDirSucceed {
		displayMessage("Created all directories successfully!")
	}

	err := walkAndCopy("etc", baseDirectory)
	if err != nil {
		displayErrorMessage(err.Error() + ": Please check if you have the right permissions!")
	} else {
		displayMessage("Initialization successful! You can start the kuiperd!")
		application.Preferences().SetBool("initialized", true)
		btnInit.Hide()
		btnStart.Show()
	}
}

func getClientIp() (string, error) {
	inters, err := net.InterfaceAddrs()

	if err != nil {
		return "0.0.0.0", err
	}

	for _, address := range inters {
		if ipNet, ok := address.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				return ipNet.IP.String(), nil
			}
		}
	}

	return "0.0.0.0", errors.New("cannot find the client IP address")
}

func walkAndCopy(path, dest string) error {
	dir, err := etc.ReadDir(path)
	if err != nil {
		return fmt.Errorf("read dir failed: %s", path)
	}

	for _, file := range dir {
		if file.IsDir() {
			_ = walkAndCopy(path+"/"+file.Name(), dest)
		} else {
			tmpPath := path + "/" + file.Name()
			dest, err := os.Create(baseDirectory + tmpPath)
			if err != nil {
				return err
			}
			sour, err := etc.ReadFile(tmpPath)
			if err != nil {
				return err
			}
			if _, err := dest.Write(sour); err != nil {
				return err
			}
		}
	}
	return nil
}

func startService() {
	go func() {
		server.StartUp(Version)
	}()
	go func() {
		time.Sleep(time.Millisecond * 500)

		restHttpType := "http"
		if conf.Config.Basic.RestTls != nil {
			restHttpType = "https"
		}
		localIPAddr, _ := getClientIp()
		msg := fmt.Sprintf("Serving kuiper (version - %s) on port %d, \nrestful API on %s://%s.", Version, conf.Config.Basic.Port, restHttpType, cast.JoinHostPortInt(localIPAddr, conf.Config.Basic.RestPort))
		displayMessage(msg)
		btnStart.Hide()
		btnQuit.Show()
	}()
}

func displayErrorMessage(message string) {
	lblMsg.SetText(message)
}

func displayMessage(message string) {
	lblMsg.SetText(message)
}

func main() {
	application = app.NewWithID("github.com/lf-edge/eKuiper")

	baseDirectory = application.Storage().RootURI().Path()

	customMetadata := application.Metadata().Custom
	if ver, exist := customMetadata["version"]; exist {
		Version = ver
	}
	application.SetIcon(fyne.NewStaticResource("account", icon))
	w := application.NewWindow("Lightweight data stream processing engine for IoT edge")
	application.SetIcon(fyne.NewStaticResource("icon", icon))

	lblMsg = widget.NewLabel("Please first in the phone permission settings\nGive this app read, write, and store permissions\nThen click [Initialize kuiperd service].")
	bannerImage := canvas.NewImageFromResource(fyne.NewStaticResource("banner", banner))
	bannerImage.FillMode = canvas.ImageFillOriginal
	bannerContainer := container.NewCenter(bannerImage)

	setDirectory()

	btnQuit = widget.NewButton("Close kuiperd service", func() {
		os.Exit(1)
	})
	btnQuit.Hide()

	initialized := application.Preferences().Bool("initialized")

	btnInit = widget.NewButton("Initialize kuiperd service", initService)
	btnStart = widget.NewButton("Start kuiperd service", startService)
	if initialized {
		btnInit.Hide()
		btnStart.Show()
	} else {
		btnInit.Show()
		btnStart.Hide()
	}

	c := container.NewVBox(bannerContainer, lblMsg, btnInit, btnStart, btnQuit)
	w.SetContent(container.NewVBox(c))
	w.Resize(fyne.NewSize(600, 600))
	w.ShowAndRun()
}
