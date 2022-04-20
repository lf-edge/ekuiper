module github.com/lf-edge/ekuiper/extensions

go 1.17

require (
	github.com/go-redis/redis/v7 v7.3.0
	github.com/influxdata/influxdb1-client v0.0.0-20200827194710-b269163b24ab
	github.com/lf-edge/ekuiper v0.0.0-20210705062157-b68b45211d6e
	github.com/mattn/go-tflite v1.0.1
	github.com/mmcloughlin/geohash v0.10.0
	github.com/nfnt/resize v0.0.0-20180221191011-83c6a9932646
	github.com/pebbe/zmq4 v1.2.7
	github.com/taosdata/driver-go/v2 v2.0.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/mattn/go-pointer v0.0.0-20190911064623-a0a44394634f // indirect
	github.com/mitchellh/mapstructure v1.4.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace github.com/lf-edge/ekuiper => ../
