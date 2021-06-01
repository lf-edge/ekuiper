module github.com/emqx/kuiper/extensions

go 1.16

require (
	github.com/emqx/kuiper v0.0.0-20210528134227-7e6a6a028a6f
	github.com/influxdata/influxdb1-client v0.0.0-20200827194710-b269163b24ab
	github.com/mattn/go-tflite v1.0.1
	github.com/mmcloughlin/geohash v0.10.0
	github.com/nfnt/resize v0.0.0-20180221191011-83c6a9932646
	github.com/pebbe/zmq4 v1.2.2
	github.com/taosdata/driver-go v0.0.0-20210525062356-2bd1b495d5f3
)

replace github.com/emqx/kuiper => ../
