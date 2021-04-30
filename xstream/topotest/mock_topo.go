package topotest

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xsql/processors"
	"github.com/emqx/kuiper/xstream"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"github.com/emqx/kuiper/xstream/planner"
	"github.com/emqx/kuiper/xstream/topotest/mockclock"
	"github.com/emqx/kuiper/xstream/topotest/mocknodes"
	"io/ioutil"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"
)

const POSTLEAP = 1000 // Time change after all data sends out
type RuleTest struct {
	Name string
	Sql  string
	R    interface{}            // The result
	M    map[string]interface{} // final metrics
	T    *xstream.PrintableTopo // printable topo, an optional field
	W    int                    // wait time for each data sending, in milli
}

var (
	DbDir    = common.GetDbDir()
	image, _ = getImg()
)

func getImg() ([]byte, string) {
	docsFolder, err := common.GetLoc("/docs/")
	if err != nil {
		common.Log.Fatalf("Cannot find docs folder: %v", err)
	}
	image, err := ioutil.ReadFile(path.Join(docsFolder, "cover.jpg"))
	if err != nil {
		common.Log.Fatalf("Cannot read image: %v", err)
	}
	b64img := base64.StdEncoding.EncodeToString(image)
	return image, b64img
}

func compareMetrics(tp *xstream.TopologyNew, m map[string]interface{}) (err error) {
	keys, values := tp.GetMetrics()
	for k, v := range m {
		var (
			index   int
			key     string
			matched bool
		)
		for index, key = range keys {
			if k == key {
				if strings.HasSuffix(k, "process_latency_us") {
					if values[index].(int64) >= v.(int64) {
						matched = true
						continue
					} else {
						break
					}
				}
				if values[index] == v {
					matched = true
				}
				break
			}
		}
		if matched {
			continue
		}
		if common.Config.Basic.Debug == true {
			for i, k := range keys {
				common.Log.Printf("%s:%v", k, values[i])
			}
		}
		//do not find
		if index < len(values) {
			return fmt.Errorf("metrics mismatch for %s:\n\nexp=%#v(%T)\n\ngot=%#v(%T)\n\n", k, v, v, values[index], values[index])
		} else {
			return fmt.Errorf("metrics mismatch for %s:\n\nexp=%#v\n\ngot=nil\n\n", k, v)
		}
	}
	return nil
}

// The time diff must larger than timeleap
var testData = map[string][]*xsql.Tuple{
	"demo": {
		{
			Emitter: "demo",
			Message: map[string]interface{}{
				"color": "red",
				"size":  3,
				"ts":    1541152486013,
			},
			Timestamp: 1541152486013,
		},
		{
			Emitter: "demo",
			Message: map[string]interface{}{
				"color": "blue",
				"size":  6,
				"ts":    1541152486822,
			},
			Timestamp: 1541152486822,
		},
		{
			Emitter: "demo",
			Message: map[string]interface{}{
				"color": "blue",
				"size":  2,
				"ts":    1541152487632,
			},
			Timestamp: 1541152487632,
		},
		{
			Emitter: "demo",
			Message: map[string]interface{}{
				"color": "yellow",
				"size":  4,
				"ts":    1541152488442,
			},
			Timestamp: 1541152488442,
		},
		{
			Emitter: "demo",
			Message: map[string]interface{}{
				"color": "red",
				"size":  1,
				"ts":    1541152489252,
			},
			Timestamp: 1541152489252,
		},
	},
	"demoError": {
		{
			Emitter: "demoError",
			Message: map[string]interface{}{
				"color": 3,
				"size":  "red",
				"ts":    1541152486013,
			},
			Timestamp: 1541152486013,
		},
		{
			Emitter: "demoError",
			Message: map[string]interface{}{
				"color": "blue",
				"size":  6,
				"ts":    "1541152486822",
			},
			Timestamp: 1541152486822,
		},
		{
			Emitter: "demoError",
			Message: map[string]interface{}{
				"color": "blue",
				"size":  2,
				"ts":    1541152487632,
			},
			Timestamp: 1541152487632,
		},
		{
			Emitter: "demoError",
			Message: map[string]interface{}{
				"color": 7,
				"size":  4,
				"ts":    1541152488442,
			},
			Timestamp: 1541152488442,
		},
		{
			Emitter: "demoError",
			Message: map[string]interface{}{
				"color": "red",
				"size":  "blue",
				"ts":    1541152489252,
			},
			Timestamp: 1541152489252,
		},
	},
	"demo1": {
		{
			Emitter: "demo1",
			Message: map[string]interface{}{
				"temp": 25.5,
				"hum":  65,
				"from": "device1",
				"ts":   1541152486013,
			},
			Timestamp: 1541152486115,
		},
		{
			Emitter: "demo1",
			Message: map[string]interface{}{
				"temp": 27.5,
				"hum":  59,
				"from": "device2",
				"ts":   1541152486823,
			},
			Timestamp: 1541152486903,
		},
		{
			Emitter: "demo1",
			Message: map[string]interface{}{
				"temp": 28.1,
				"hum":  75,
				"from": "device3",
				"ts":   1541152487632,
			},
			Timestamp: 1541152487702,
		},
		{
			Emitter: "demo1",
			Message: map[string]interface{}{
				"temp": 27.4,
				"hum":  80,
				"from": "device1",
				"ts":   1541152488442,
			},
			Timestamp: 1541152488605,
		},
		{
			Emitter: "demo1",
			Message: map[string]interface{}{
				"temp": 25.5,
				"hum":  62,
				"from": "device3",
				"ts":   1541152489252,
			},
			Timestamp: 1541152489305,
		},
	},
	"sessionDemo": {
		{
			Emitter: "sessionDemo",
			Message: map[string]interface{}{
				"temp": 25.5,
				"hum":  65,
				"ts":   1541152486013,
			},
			Timestamp: 1541152486013,
		},
		{
			Emitter: "sessionDemo",
			Message: map[string]interface{}{
				"temp": 27.5,
				"hum":  59,
				"ts":   1541152486823,
			},
			Timestamp: 1541152486823,
		},
		{
			Emitter: "sessionDemo",
			Message: map[string]interface{}{
				"temp": 28.1,
				"hum":  75,
				"ts":   1541152487932,
			},
			Timestamp: 1541152487932,
		},
		{
			Emitter: "sessionDemo",
			Message: map[string]interface{}{
				"temp": 27.4,
				"hum":  80,
				"ts":   1541152488442,
			},
			Timestamp: 1541152488442,
		},
		{
			Emitter: "sessionDemo",
			Message: map[string]interface{}{
				"temp": 25.5,
				"hum":  62,
				"ts":   1541152489252,
			},
			Timestamp: 1541152489252,
		},
		{
			Emitter: "sessionDemo",
			Message: map[string]interface{}{
				"temp": 26.2,
				"hum":  63,
				"ts":   1541152490062,
			},
			Timestamp: 1541152490062,
		},
		{
			Emitter: "sessionDemo",
			Message: map[string]interface{}{
				"temp": 26.8,
				"hum":  71,
				"ts":   1541152490872,
			},
			Timestamp: 1541152490872,
		},
		{
			Emitter: "sessionDemo",
			Message: map[string]interface{}{
				"temp": 28.9,
				"hum":  85,
				"ts":   1541152491682,
			},
			Timestamp: 1541152491682,
		},
		{
			Emitter: "sessionDemo",
			Message: map[string]interface{}{
				"temp": 29.1,
				"hum":  92,
				"ts":   1541152492492,
			},
			Timestamp: 1541152492492,
		},
		{
			Emitter: "sessionDemo",
			Message: map[string]interface{}{
				"temp": 32.2,
				"hum":  99,
				"ts":   1541152493202,
			},
			Timestamp: 1541152493202,
		},
		{
			Emitter: "sessionDemo",
			Message: map[string]interface{}{
				"temp": 30.9,
				"hum":  87,
				"ts":   1541152494112,
			},
			Timestamp: 1541152494112,
		},
	},
	"demoE": {
		{
			Emitter: "demoE",
			Message: map[string]interface{}{
				"color": "red",
				"size":  3,
				"ts":    1541152486013,
			},
			Timestamp: 1541152486023,
		},
		{
			Emitter: "demoE",
			Message: map[string]interface{}{
				"color": "blue",
				"size":  2,
				"ts":    1541152487632,
			},
			Timestamp: 1541152487822,
		},
		{
			Emitter: "demoE",
			Message: map[string]interface{}{
				"color": "red",
				"size":  1,
				"ts":    1541152489252,
			},
			Timestamp: 1541152489632,
		},
		{ //dropped item
			Emitter: "demoE",
			Message: map[string]interface{}{
				"color": "blue",
				"size":  6,
				"ts":    1541152486822,
			},
			Timestamp: 1541152489842,
		},
		{
			Emitter: "demoE",
			Message: map[string]interface{}{
				"color": "yellow",
				"size":  4,
				"ts":    1541152488442,
			},
			Timestamp: 1541152490052,
		},
		{ //To lift the watermark and issue all windows
			Emitter: "demoE",
			Message: map[string]interface{}{
				"color": "yellow",
				"size":  4,
				"ts":    1541152492342,
			},
			Timestamp: 1541152498888,
		},
	},
	"demo1E": {
		{
			Emitter: "demo1E",
			Message: map[string]interface{}{
				"temp": 27.5,
				"hum":  59,
				"ts":   1541152486823,
			},
			Timestamp: 1541152487250,
		},
		{
			Emitter: "demo1E",
			Message: map[string]interface{}{
				"temp": 25.5,
				"hum":  65,
				"ts":   1541152486013,
			},
			Timestamp: 1541152487751,
		},
		{
			Emitter: "demo1E",
			Message: map[string]interface{}{
				"temp": 27.4,
				"hum":  80,
				"ts":   1541152488442,
			},
			Timestamp: 1541152489252,
		},
		{
			Emitter: "demo1E",
			Message: map[string]interface{}{
				"temp": 28.1,
				"hum":  75,
				"ts":   1541152487632,
			},
			Timestamp: 1541152489753,
		},
		{
			Emitter: "demo1E",
			Message: map[string]interface{}{
				"temp": 25.5,
				"hum":  62,
				"ts":   1541152489252,
			},
			Timestamp: 1541152489954,
		},
		{
			Emitter: "demo1E",
			Message: map[string]interface{}{
				"temp": 25.5,
				"hum":  62,
				"ts":   1541152499252,
			},
			Timestamp: 1541152499755,
		},
	},
	"sessionDemoE": {
		{
			Emitter: "sessionDemoE",
			Message: map[string]interface{}{
				"temp": 25.5,
				"hum":  65,
				"ts":   1541152486013,
			},
			Timestamp: 1541152486250,
		},
		{
			Emitter: "sessionDemoE",
			Message: map[string]interface{}{
				"temp": 28.1,
				"hum":  75,
				"ts":   1541152487932,
			},
			Timestamp: 1541152487951,
		},
		{
			Emitter: "sessionDemoE",
			Message: map[string]interface{}{
				"temp": 27.5,
				"hum":  59,
				"ts":   1541152486823,
			},
			Timestamp: 1541152488552,
		},
		{
			Emitter: "sessionDemoE",
			Message: map[string]interface{}{
				"temp": 25.5,
				"hum":  62,
				"ts":   1541152489252,
			},
			Timestamp: 1541152489353,
		},
		{
			Emitter: "sessionDemoE",
			Message: map[string]interface{}{
				"temp": 27.4,
				"hum":  80,
				"ts":   1541152488442,
			},
			Timestamp: 1541152489854,
		},
		{
			Emitter: "sessionDemoE",
			Message: map[string]interface{}{
				"temp": 26.2,
				"hum":  63,
				"ts":   1541152490062,
			},
			Timestamp: 1541152490155,
		},
		{
			Emitter: "sessionDemoE",
			Message: map[string]interface{}{
				"temp": 28.9,
				"hum":  85,
				"ts":   1541152491682,
			},
			Timestamp: 1541152491686,
		},
		{
			Emitter: "sessionDemoE",
			Message: map[string]interface{}{
				"temp": 26.8,
				"hum":  71,
				"ts":   1541152490872,
			},
			Timestamp: 1541152491972,
		},
		{
			Emitter: "sessionDemoE",
			Message: map[string]interface{}{
				"temp": 29.1,
				"hum":  92,
				"ts":   1541152492492,
			},
			Timestamp: 1541152492592,
		},
		{
			Emitter: "sessionDemoE",
			Message: map[string]interface{}{
				"temp": 30.9,
				"hum":  87,
				"ts":   1541152494112,
			},
			Timestamp: 1541152494212,
		},
		{
			Emitter: "sessionDemoE",
			Message: map[string]interface{}{
				"temp": 32.2,
				"hum":  99,
				"ts":   1541152493202,
			},
			Timestamp: 1541152495202,
		},
		{
			Emitter: "sessionDemoE",
			Message: map[string]interface{}{
				"temp": 32.2,
				"hum":  99,
				"ts":   1541152499202,
			},
			Timestamp: 1541152499402,
		},
	},
	"demoErr": {
		{
			Emitter: "demoErr",
			Message: map[string]interface{}{
				"color": "red",
				"size":  3,
				"ts":    1541152486013,
			},
			Timestamp: 1541152486221,
		},
		{
			Emitter: "demoErr",
			Message: map[string]interface{}{
				"color": 2,
				"size":  "blue",
				"ts":    1541152487632,
			},
			Timestamp: 1541152487722,
		},
		{
			Emitter: "demoErr",
			Message: map[string]interface{}{
				"color": "red",
				"size":  1,
				"ts":    1541152489252,
			},
			Timestamp: 1541152489332,
		},
		{ //dropped item
			Emitter: "demoErr",
			Message: map[string]interface{}{
				"color": "blue",
				"size":  6,
				"ts":    1541152486822,
			},
			Timestamp: 1541152489822,
		},
		{
			Emitter: "demoErr",
			Message: map[string]interface{}{
				"color": "yellow",
				"size":  4,
				"ts":    1541152488442,
			},
			Timestamp: 1541152490042,
		},
		{ //To lift the watermark and issue all windows
			Emitter: "demoErr",
			Message: map[string]interface{}{
				"color": "yellow",
				"size":  4,
				"ts":    1541152492342,
			},
			Timestamp: 1541152493842,
		},
	},
	"ldemo": {
		{
			Emitter: "ldemo",
			Message: map[string]interface{}{
				"color": "red",
				"size":  3,
				"ts":    1541152486013,
			},
			Timestamp: 1541152486013,
		},
		{
			Emitter: "ldemo",
			Message: map[string]interface{}{
				"color": "blue",
				"size":  "string",
				"ts":    1541152486822,
			},
			Timestamp: 1541152486822,
		},
		{
			Emitter: "ldemo",
			Message: map[string]interface{}{
				"size": 3,
				"ts":   1541152487632,
			},
			Timestamp: 1541152487632,
		},
		{
			Emitter: "ldemo",
			Message: map[string]interface{}{
				"color": 49,
				"size":  2,
				"ts":    1541152488442,
			},
			Timestamp: 1541152488442,
		},
		{
			Emitter: "ldemo",
			Message: map[string]interface{}{
				"color": "red",
				"ts":    1541152489252,
			},
			Timestamp: 1541152489252,
		},
	},
	"ldemo1": {
		{
			Emitter: "ldemo1",
			Message: map[string]interface{}{
				"temp": 25.5,
				"hum":  65,
				"ts":   1541152486013,
			},
			Timestamp: 1541152486013,
		},
		{
			Emitter: "ldemo1",
			Message: map[string]interface{}{
				"temp": 27.5,
				"hum":  59,
				"ts":   1541152486823,
			},
			Timestamp: 1541152486823,
		},
		{
			Emitter: "ldemo1",
			Message: map[string]interface{}{
				"temp": 28.1,
				"hum":  75,
				"ts":   1541152487632,
			},
			Timestamp: 1541152487632,
		},
		{
			Emitter: "ldemo1",
			Message: map[string]interface{}{
				"temp": 27.4,
				"hum":  80,
				"ts":   "1541152488442",
			},
			Timestamp: 1541152488442,
		},
		{
			Emitter: "ldemo1",
			Message: map[string]interface{}{
				"temp": 25.5,
				"hum":  62,
				"ts":   1541152489252,
			},
			Timestamp: 1541152489252,
		},
	},
	"lsessionDemo": {
		{
			Emitter: "lsessionDemo",
			Message: map[string]interface{}{
				"temp": 25.5,
				"hum":  65,
				"ts":   1541152486013,
			},
			Timestamp: 1541152486013,
		},
		{
			Emitter: "lsessionDemo",
			Message: map[string]interface{}{
				"temp": 27.5,
				"hum":  59,
				"ts":   1541152486823,
			},
			Timestamp: 1541152486823,
		},
		{
			Emitter: "lsessionDemo",
			Message: map[string]interface{}{
				"temp": 28.1,
				"hum":  75,
				"ts":   1541152487932,
			},
			Timestamp: 1541152487932,
		},
		{
			Emitter: "lsessionDemo",
			Message: map[string]interface{}{
				"temp": 27.4,
				"hum":  80,
				"ts":   1541152488442,
			},
			Timestamp: 1541152488442,
		},
		{
			Emitter: "lsessionDemo",
			Message: map[string]interface{}{
				"temp": 25.5,
				"hum":  62,
				"ts":   1541152489252,
			},
			Timestamp: 1541152489252,
		},
		{
			Emitter: "lsessionDemo",
			Message: map[string]interface{}{
				"temp": 26.2,
				"hum":  63,
				"ts":   1541152490062,
			},
			Timestamp: 1541152490062,
		},
		{
			Emitter: "lsessionDemo",
			Message: map[string]interface{}{
				"temp": 26.8,
				"hum":  71,
				"ts":   1541152490872,
			},
			Timestamp: 1541152490872,
		},
		{
			Emitter: "lsessionDemo",
			Message: map[string]interface{}{
				"temp": 28.9,
				"hum":  85,
				"ts":   1541152491682,
			},
			Timestamp: 1541152491682,
		},
		{
			Emitter: "lsessionDemo",
			Message: map[string]interface{}{
				"temp": 29.1,
				"hum":  92,
				"ts":   1541152492492,
			},
			Timestamp: 1541152492492,
		},
		{
			Emitter: "lsessionDemo",
			Message: map[string]interface{}{
				"temp": 2.2,
				"hum":  99,
				"ts":   1541152493202,
			},
			Timestamp: 1541152493202,
		},
		{
			Emitter: "lsessionDemo",
			Message: map[string]interface{}{
				"temp": 30.9,
				"hum":  87,
				"ts":   1541152494112,
			},
			Timestamp: 1541152494112,
		},
	},
	"text": {
		{
			Emitter: "text",
			Message: map[string]interface{}{
				"slogan": "Impossible is nothing",
				"brand":  "Adidas",
			},
			Timestamp: 1541152486500,
		},
		{
			Emitter: "text",
			Message: map[string]interface{}{
				"slogan": "Stronger than dirt",
				"brand":  "Ajax",
			},
			Timestamp: 1541152487400,
		},
		{
			Emitter: "text",
			Message: map[string]interface{}{
				"slogan": "Belong anywhere",
				"brand":  "Airbnb",
			},
			Timestamp: 1541152488300,
		},
		{
			Emitter: "text",
			Message: map[string]interface{}{
				"slogan": "I can'T believe I ate the whole thing",
				"brand":  "Alka Seltzer",
			},
			Timestamp: 1541152489200,
		},
		{
			Emitter: "text",
			Message: map[string]interface{}{
				"slogan": "You're in good hands",
				"brand":  "Allstate",
			},
			Timestamp: 1541152490100,
		},
		{
			Emitter: "text",
			Message: map[string]interface{}{
				"slogan": "Don'T leave home without it",
				"brand":  "American Express",
			},
			Timestamp: 1541152491200,
		},
		{
			Emitter: "text",
			Message: map[string]interface{}{
				"slogan": "Think different",
				"brand":  "Apple",
			},
			Timestamp: 1541152492300,
		},
		{
			Emitter: "text",
			Message: map[string]interface{}{
				"slogan": "We try harder",
				"brand":  "Avis",
			},
			Timestamp: 1541152493400,
		},
	},
	"binDemo": {
		{
			Emitter: "binDemo",
			Message: map[string]interface{}{
				"self": image,
			},
			Timestamp: 1541152486013,
		},
	},
	"fakeBin": {
		{
			Emitter: "fakeBin",
			Message: map[string]interface{}{
				"self": []byte("golang"),
			},
			Timestamp: 1541152486013,
		},
		{
			Emitter: "fakeBin",
			Message: map[string]interface{}{
				"self": []byte("peacock"),
			},
			Timestamp: 1541152487013,
		},
		{
			Emitter: "fakeBin",
			Message: map[string]interface{}{
				"self": []byte("bullfrog"),
			},
			Timestamp: 1541152488013,
		},
	},
	"helloStr": {
		{
			Emitter: "helloStr",
			Message: map[string]interface{}{
				"Name": "world",
			},
			Timestamp: 1541152486013,
		},
		{
			Emitter: "helloStr",
			Message: map[string]interface{}{
				"Name": "golang",
			},
			Timestamp: 1541152487013,
		},
		{
			Emitter: "helloStr",
			Message: map[string]interface{}{
				"Name": "peacock",
			},
			Timestamp: 1541152488013,
		},
	},
	"commands": {
		{
			Emitter: "commands",
			Message: map[string]interface{}{
				"cmd":        "get",
				"base64_img": "my image",
			},
			Timestamp: 1541152486013,
		},
		{
			Emitter: "commands",
			Message: map[string]interface{}{
				"cmd":        "detect",
				"base64_img": "my image",
			},
			Timestamp: 1541152487013,
		},
		{
			Emitter: "commands",
			Message: map[string]interface{}{
				"cmd":        "delete",
				"base64_img": "my image",
			},
			Timestamp: 1541152488013,
		},
	},
}

func commonResultFunc(result [][]byte) interface{} {
	var maps [][]map[string]interface{}
	for _, v := range result {
		var mapRes []map[string]interface{}
		err := json.Unmarshal(v, &mapRes)
		if err != nil {
			panic("Failed to parse the input into map")
		}
		maps = append(maps, mapRes)
	}
	return maps
}

func DoRuleTest(t *testing.T, tests []RuleTest, j int, opt *api.RuleOption) {
	doRuleTestBySinkProps(t, tests, j, opt, nil, commonResultFunc)
}

func doRuleTestBySinkProps(t *testing.T, tests []RuleTest, j int, opt *api.RuleOption, sinkProps map[string]interface{}, resultFunc func(result [][]byte) interface{}) {
	fmt.Printf("The test bucket for option %d size is %d.\n\n", j, len(tests))
	for i, tt := range tests {
		datas, dataLength, tp, mockSink, errCh := createStream(t, tt, j, opt, sinkProps)
		if tp == nil {
			t.Errorf("topo is not created successfully")
			break
		}
		wait := tt.W
		if wait == 0 {
			wait = 5
		}
		switch opt.Qos {
		case api.ExactlyOnce:
			wait *= 4
		case api.AtLeastOnce:
			wait *= 3
		}
		var retry int
		if opt.Qos > api.AtMostOnce {
			for retry = 3; retry > 0; retry-- {
				if tp.GetCoordinator() == nil || !tp.GetCoordinator().IsActivated() {
					common.Log.Debugf("waiting for coordinator ready %d\n", retry)
					time.Sleep(10 * time.Millisecond)
				} else {
					break
				}
			}
			if retry < 0 {
				t.Error("coordinator timeout")
				t.FailNow()
			}
		}
		if err := sendData(t, dataLength, tt.M, datas, errCh, tp, POSTLEAP, wait); err != nil {
			t.Errorf("send data error %s", err)
			break
		}
		compareResult(t, mockSink, resultFunc, tt, i, tp)
	}
}

func compareResult(t *testing.T, mockSink *mocknodes.MockSink, resultFunc func(result [][]byte) interface{}, tt RuleTest, i int, tp *xstream.TopologyNew) {
	// Check results
	results := mockSink.GetResults()
	maps := resultFunc(results)

	if !reflect.DeepEqual(tt.R, maps) {
		t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.Sql, tt.R, maps)
	}
	if err := compareMetrics(tp, tt.M); err != nil {
		t.Errorf("%d. %q\n\nmetrics mismatch:\n\n%s\n\n", i, tt.Sql, err)
	}
	if tt.T != nil {
		topo := tp.GetTopo()
		if !reflect.DeepEqual(tt.T, topo) {
			t.Errorf("%d. %q\n\ntopo mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.Sql, tt.T, topo)
		}
	}
	tp.Cancel()
}

func sendData(t *testing.T, dataLength int, metrics map[string]interface{}, datas [][]*xsql.Tuple, errCh <-chan error, tp *xstream.TopologyNew, postleap int, wait int) error {
	// Send data and move time
	mockClock := mockclock.GetMockClock()
	// Set the current time
	mockClock.Add(0)
	// TODO assume multiple data source send the data in order and has the same length
	for i := 0; i < dataLength; i++ {
		for _, d := range datas {
			// Make sure time is going forward only
			// gradually add up time to ensure checkpoint is triggered before the data send
			for n := common.GetNowInMilli() + 100; d[i].Timestamp+100 > n; n += 100 {
				if d[i].Timestamp < n {
					n = d[i].Timestamp
				}
				mockClock.Set(common.TimeFromUnixMilli(n))
				common.Log.Debugf("Clock set to %d", common.GetNowInMilli())
				time.Sleep(1)
			}
			select {
			case err := <-errCh:
				t.Log(err)
				tp.Cancel()
				return err
			default:
			}
			time.Sleep(time.Duration(wait) * time.Millisecond)
		}
	}
	mockClock.Add(time.Duration(postleap) * time.Millisecond)
	common.Log.Debugf("Clock add to %d", common.GetNowInMilli())
	// Check if stream done. Poll for metrics,
	time.Sleep(10 * time.Millisecond)
	var retry int
	for retry = 4; retry > 0; retry-- {
		if err := compareMetrics(tp, metrics); err == nil {
			break
		} else {
			common.Log.Errorf("check metrics error at %d: %s", retry, err)
		}
		time.Sleep(1000 * time.Millisecond)
	}
	if retry == 0 {
		t.Error("send data timeout")
	} else if retry < 2 {
		common.Log.Debugf("try %d for metric comparison\n", 2-retry)
	}
	return nil
}

func createStream(t *testing.T, tt RuleTest, j int, opt *api.RuleOption, sinkProps map[string]interface{}) ([][]*xsql.Tuple, int, *xstream.TopologyNew, *mocknodes.MockSink, <-chan error) {
	mockclock.ResetClock(1541152486000)
	// Create stream
	var (
		sources    []*nodes.SourceNode
		datas      [][]*xsql.Tuple
		dataLength int
	)

	parser := xsql.NewParser(strings.NewReader(tt.Sql))
	if stmt, err := xsql.Language.Parse(parser); err != nil {
		t.Errorf("parse sql %s error: %s", tt.Sql, err)
	} else {
		if selectStmt, ok := stmt.(*xsql.SelectStatement); !ok {
			t.Errorf("sql %s is not a select statement", tt.Sql)
		} else {
			streams := xsql.GetStreams(selectStmt)
			for _, stream := range streams {
				data, ok := testData[stream]
				if !ok {
					continue
				}
				dataLength = len(data)
				datas = append(datas, data)
				source := nodes.NewSourceNodeWithSource(stream, mocknodes.NewMockSource(data), map[string]string{
					"DATASOURCE": stream,
				})
				sources = append(sources, source)
			}
		}
	}
	mockSink := mocknodes.NewMockSink()
	sink := nodes.NewSinkNodeWithSink("mockSink", mockSink, sinkProps)
	tp, err := planner.PlanWithSourcesAndSinks(&api.Rule{Id: fmt.Sprintf("%s_%d", tt.Name, j), Sql: tt.Sql, Options: opt}, DbDir, sources, []*nodes.SinkNode{sink})
	if err != nil {
		t.Error(err)
		return nil, 0, nil, nil, nil
	}
	errCh := tp.Open()
	return datas, dataLength, tp, mockSink, errCh
}

// Create or drop streams
func HandleStream(createOrDrop bool, names []string, t *testing.T) {
	p := processors.NewStreamProcessor(path.Join(DbDir, "stream"))
	for _, name := range names {
		var sql string
		if createOrDrop {
			switch name {
			case "demo":
				sql = `CREATE STREAM demo (
					color STRING,
					size BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demo", FORMAT="json", KEY="ts");`
			case "demoError":
				sql = `CREATE STREAM demoError (
					color STRING,
					size BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demoError", FORMAT="json", KEY="ts");`
			case "demo1":
				sql = `CREATE STREAM demo1 (
					temp FLOAT,
					hum BIGINT,` +
					"`from`" + ` STRING,
					ts BIGINT
				) WITH (DATASOURCE="demo1", FORMAT="json", KEY="ts");`
			case "sessionDemo":
				sql = `CREATE STREAM sessionDemo (
					temp FLOAT,
					hum BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="sessionDemo", FORMAT="json", KEY="ts");`
			case "demoE":
				sql = `CREATE STREAM demoE (
					color STRING,
					size BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demoE", FORMAT="json", KEY="ts", TIMESTAMP="ts");`
			case "demo1E":
				sql = `CREATE STREAM demo1E (
					temp FLOAT,
					hum BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demo1E", FORMAT="json", KEY="ts", TIMESTAMP="ts");`
			case "sessionDemoE":
				sql = `CREATE STREAM sessionDemoE (
					temp FLOAT,
					hum BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="sessionDemoE", FORMAT="json", KEY="ts", TIMESTAMP="ts");`
			case "demoErr":
				sql = `CREATE STREAM demoErr (
					color STRING,
					size BIGINT,
					ts BIGINT
				) WITH (DATASOURCE="demoErr", FORMAT="json", KEY="ts", TIMESTAMP="ts");`
			case "ldemo":
				sql = `CREATE STREAM ldemo (					
				) WITH (DATASOURCE="ldemo", FORMAT="json");`
			case "ldemo1":
				sql = `CREATE STREAM ldemo1 (
				) WITH (DATASOURCE="ldemo1", FORMAT="json");`
			case "lsessionDemo":
				sql = `CREATE STREAM lsessionDemo (
				) WITH (DATASOURCE="lsessionDemo", FORMAT="json");`
			case "ext":
				sql = "CREATE STREAM ext (count bigint) WITH (DATASOURCE=\"users\", FORMAT=\"JSON\", TYPE=\"random\", CONF_KEY=\"ext\")"
			case "ext2":
				sql = "CREATE STREAM ext2 (count bigint) WITH (DATASOURCE=\"users\", FORMAT=\"JSON\", TYPE=\"random\", CONF_KEY=\"dedup\")"
			case "text":
				sql = "CREATE STREAM text (slogan string, brand string) WITH (DATASOURCE=\"users\", FORMAT=\"JSON\")"
			case "binDemo":
				sql = "CREATE STREAM binDemo () WITH (DATASOURCE=\"users\", FORMAT=\"BINARY\")"
			case "table1":
				sql = `CREATE TABLE table1 (
					name STRING,
					size BIGINT,
					id BIGINT
				) WITH (DATASOURCE="lookup.json", FORMAT="json", CONF_KEY="test");`
			case "helloStr":
				sql = `CREATE STREAM helloStr (name string) WITH (DATASOURCE="hello", FORMAT="JSON")`
			case "commands":
				sql = `CREATE STREAM commands (cmd string, base64_img string) WITH (DATASOURCE="commands", FORMAT="JSON")`
			case "fakeBin":
				sql = "CREATE STREAM fakeBin () WITH (DATASOURCE=\"users\", FORMAT=\"BINARY\")"
			default:
				t.Errorf("create stream %s fail", name)
			}
		} else {
			if strings.Index(name, "table") == 0 {
				sql = `DROP TABLE ` + name
			} else {
				sql = `DROP STREAM ` + name
			}
		}

		_, err := p.ExecStmt(sql)
		if err != nil {
			t.Log(err)
		}
	}
}

type RuleCheckpointTest struct {
	RuleTest
	PauseSize   int                    // Stop stream after sending pauseSize source to test checkpoint resume
	Cc          int                    // checkpoint count when paused
	PauseMetric map[string]interface{} // The metric to check when paused
}

func DoCheckpointRuleTest(t *testing.T, tests []RuleCheckpointTest, j int, opt *api.RuleOption) {
	fmt.Printf("The test bucket for option %d size is %d.\n\n", j, len(tests))
	for i, tt := range tests {
		datas, dataLength, tp, mockSink, errCh := createStream(t, tt.RuleTest, j, opt, nil)
		if tp == nil {
			t.Errorf("topo is not created successfully")
			break
		}
		var retry int
		for retry = 10; retry > 0; retry-- {
			if tp.GetCoordinator() == nil || !tp.GetCoordinator().IsActivated() {
				common.Log.Debugf("waiting for coordinator ready %d\n", retry)
				time.Sleep(10 * time.Millisecond)
			} else {
				break
			}
		}
		if retry == 0 {
			t.Error("coordinator timeout")
			t.FailNow()
		}
		common.Log.Debugf("Start sending first phase data done at %d", common.GetNowInMilli())
		if err := sendData(t, tt.PauseSize, tt.PauseMetric, datas, errCh, tp, 100, 100); err != nil {
			t.Errorf("first phase send data error %s", err)
			break
		}
		common.Log.Debugf("Send first phase data done at %d", common.GetNowInMilli())
		// compare checkpoint count
		time.Sleep(10 * time.Millisecond)
		for retry = 3; retry > 0; retry-- {
			actual := tp.GetCoordinator().GetCompleteCount()
			if tt.Cc == actual {
				break
			} else {
				common.Log.Debugf("check checkpointCount error at %d: %d\n", retry, actual)
			}
			time.Sleep(200 * time.Millisecond)
		}
		cc := tp.GetCoordinator().GetCompleteCount()
		tp.Cancel()
		if retry == 0 {
			t.Errorf("%d-%d. checkpoint count\n\nresult mismatch:\n\nexp=%#v\n\ngot=%d\n\n", i, j, tt.Cc, cc)
			return
		} else if retry < 3 {
			common.Log.Debugf("try %d for checkpoint count\n", 4-retry)
		}
		tp.Cancel()
		time.Sleep(10 * time.Millisecond)
		// resume stream
		common.Log.Debugf("Resume stream at %d", common.GetNowInMilli())
		errCh = tp.Open()
		common.Log.Debugf("After open stream at %d", common.GetNowInMilli())
		if err := sendData(t, dataLength, tt.M, datas, errCh, tp, POSTLEAP, 10); err != nil {
			t.Errorf("second phase send data error %s", err)
			break
		}
		compareResult(t, mockSink, commonResultFunc, tt.RuleTest, i, tp)
	}
}

func CreateRule(name, sql string) (*api.Rule, error) {
	p := processors.NewRuleProcessor(DbDir)
	p.ExecDrop(name)
	return p.ExecCreate(name, sql)
}
