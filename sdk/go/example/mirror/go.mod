module github.com/lf-edge/ekuiper-plugin-mirror

require (
	github.com/lf-edge/ekuiper/sdk/go v0.0.0-20210916082120-031cd83a7fd8
	github.com/mitchellh/mapstructure v1.4.1
)

require (
	github.com/Microsoft/go-winio v0.4.11 // indirect
	github.com/keepeye/logrus-filename v0.0.0-20190711075016-ce01a4391dd1 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	go.nanomsg.org/mangos/v3 v3.2.1 // indirect
	golang.org/x/sys v0.0.0-20210510120138-977fb7262007 // indirect
)

replace github.com/lf-edge/ekuiper/sdk/go => ../../

go 1.17
