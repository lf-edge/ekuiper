module github.com/lf-edge/ekuiper

require (
	github.com/Masterminds/sprig/v3 v3.2.1
	github.com/PaesslerAG/gval v1.0.0
	github.com/PaesslerAG/jsonpath v0.1.1
	github.com/alicebob/miniredis/v2 v2.15.1
	github.com/benbjohnson/clock v1.0.0
	github.com/cespare/xxhash/v2 v2.1.1 // indirect
	github.com/eclipse/paho.mqtt.golang v1.3.5
	github.com/edgexfoundry/go-mod-core-contracts/v2 v2.0.0
	github.com/edgexfoundry/go-mod-messaging/v2 v2.0.1
	github.com/fastly/go-utils v0.0.0-20180712184237-d95a45783239 // indirect
	github.com/gdexlab/go-render v1.0.1
	github.com/golang-collections/collections v0.0.0-20130729185459-604e922904d3
	github.com/golang/protobuf v1.5.2
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/google/uuid v1.2.0
	github.com/gorilla/handlers v1.4.2
	github.com/gorilla/mux v1.7.3
	github.com/huandu/xstrings v1.3.2 // indirect
	github.com/imdario/mergo v0.3.12 // indirect
	github.com/jehiah/go-strftime v0.0.0-20171201141054-1d33003b3869 // indirect
	github.com/jhump/protoreflect v1.8.2
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/keepeye/logrus-filename v0.0.0-20190711075016-ce01a4391dd1
	github.com/lestrrat-go/file-rotatelogs v2.4.0+incompatible
	github.com/lestrrat-go/strftime v1.0.3 // indirect
	github.com/mattn/go-sqlite3 v1.14.5
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/mapstructure v1.4.1
	github.com/msgpack-rpc/msgpack-rpc-go v0.0.0-20131026060856-c76397e1782b
	github.com/msgpack/msgpack-go v0.0.0-20130625150338-8224460e6fa3 // indirect
	github.com/nfnt/resize v0.0.0-20180221191011-83c6a9932646 // indirect
	github.com/pebbe/zmq4 v1.2.7
	github.com/prometheus/client_golang v1.2.1
	github.com/sirupsen/logrus v1.4.2
	github.com/smartystreets/goconvey v1.6.4 // indirect
	github.com/tebeka/strftime v0.1.5 // indirect
	github.com/ugorji/go/codec v1.2.5
	github.com/urfave/cli v1.22.0
	go.nanomsg.org/mangos/v3 v3.2.1
	golang.org/x/net v0.0.0-20210405180319-a5a99cb37ef4
	google.golang.org/genproto v0.0.0-20210602131652-f16073e35f0c
	google.golang.org/grpc v1.38.0
	google.golang.org/protobuf v1.26.0
	gopkg.in/ini.v1 v1.62.0
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b
)

replace github.com/lf-edge/ekuiper/extensions => ./extensions

go 1.16
