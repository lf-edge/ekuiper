package processors

import (
	"encoding/json"
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/nodes"
	"github.com/emqx/kuiper/xstream/test"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"
)

const SOURCELEAP = 200 // Time change before sending a data
const POSTLEAP = 1000  // Time change after all data sends out
type ruleTest struct {
	name string
	sql  string
	r    interface{}            // The result
	m    map[string]interface{} // final metrics
	t    *xstream.PrintableTopo // printable topo, an optional field
}

var DbDir = getDbDir()

func getDbDir() string {
	common.InitConf()
	dbDir, err := common.GetDataLoc()
	if err != nil {
		log.Panic(err)
	}
	log.Infof("db location is %s", dbDir)
	return dbDir
}

func cleanStateData() {
	dbDir, err := common.GetDataLoc()
	if err != nil {
		log.Panic(err)
	}
	c := path.Join(dbDir, "checkpoints")
	err = os.RemoveAll(c)
	if err != nil {
		log.Errorf("%s", err)
	}
	s := path.Join(dbDir, "sink", "cache")
	err = os.RemoveAll(s)
	if err != nil {
		log.Errorf("%s", err)
	}
}

func compareMetrics(tp *xstream.TopologyNew, m map[string]interface{}) (err error) {
	keys, values := tp.GetMetrics()
	if common.Config.Basic.Debug == true {
		for i, k := range keys {
			log.Printf("%s:%v", k, values[i])
		}
	}
	for k, v := range m {
		var (
			index   int
			key     string
			matched bool
		)
		for index, key = range keys {
			if k == key {
				if strings.HasSuffix(k, "process_latency_ms") {
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
		//do not find
		if index < len(values) {
			return fmt.Errorf("metrics mismatch for %s:\n\nexp=%#v(%t)\n\ngot=%#v(%t)\n\n", k, v, v, values[index], values[index])
		} else {
			return fmt.Errorf("metrics mismatch for %s:\n\nexp=%#v\n\ngot=nil\n\n", k, v)
		}
	}
	return nil
}

func errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
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
			Timestamp: 1541152486013,
		},
		{
			Emitter: "demo1",
			Message: map[string]interface{}{
				"temp": 27.5,
				"hum":  59,
				"from": "device2",
				"ts":   1541152486823,
			},
			Timestamp: 1541152486823,
		},
		{
			Emitter: "demo1",
			Message: map[string]interface{}{
				"temp": 28.1,
				"hum":  75,
				"from": "device3",
				"ts":   1541152487632,
			},
			Timestamp: 1541152487632,
		},
		{
			Emitter: "demo1",
			Message: map[string]interface{}{
				"temp": 27.4,
				"hum":  80,
				"from": "device1",
				"ts":   1541152488442,
			},
			Timestamp: 1541152488442,
		},
		{
			Emitter: "demo1",
			Message: map[string]interface{}{
				"temp": 25.5,
				"hum":  62,
				"from": "device3",
				"ts":   1541152489252,
			},
			Timestamp: 1541152489252,
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
				"slogan": "I can't believe I ate the whole thing",
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
				"slogan": "Don't leave home without it",
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

func doRuleTest(t *testing.T, tests []ruleTest, j int, opt *api.RuleOption) {
	doRuleTestBySinkProps(t, tests, j, opt, nil, commonResultFunc)
}

func doRuleTestBySinkProps(t *testing.T, tests []ruleTest, j int, opt *api.RuleOption, sinkProps map[string]interface{}, resultFunc func(result [][]byte) interface{}) {
	fmt.Printf("The test bucket for option %d size is %d.\n\n", j, len(tests))
	for i, tt := range tests {
		datas, dataLength, tp, mockSink, errCh := createStream(t, tt, j, opt, sinkProps)
		if err := sendData(t, dataLength, tt.m, datas, errCh, tp, POSTLEAP); err != nil {
			t.Errorf("send data error %s", err)
			break
		}
		compareResult(t, mockSink, resultFunc, tt, i, tp)
	}
}

func compareResult(t *testing.T, mockSink *test.MockSink, resultFunc func(result [][]byte) interface{}, tt ruleTest, i int, tp *xstream.TopologyNew) {
	// Check results
	results := mockSink.GetResults()
	maps := resultFunc(results)

	if !reflect.DeepEqual(tt.r, maps) {
		t.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.r, maps)
	}
	//if err := compareMetrics(tp, tt.m); err != nil {
	//	t.Errorf("%d. %q\n\nmetrics mismatch:\n\n%s\n\n", i, tt.sql, err)
	//}
	if tt.t != nil {
		topo := tp.GetTopo()
		if !reflect.DeepEqual(tt.t, topo) {
			t.Errorf("%d. %q\n\ntopo mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, tt.sql, tt.t, topo)
		}
	}
	tp.Cancel()
}

func sendData(t *testing.T, dataLength int, metrics map[string]interface{}, datas [][]*xsql.Tuple, errCh <-chan error, tp *xstream.TopologyNew, postleap int) error {
	// Send data and move time
	mockClock := test.GetMockClock()
	// TODO assume multiple data source send the data in order and has the same length
	for i := 0; i < dataLength; i++ {
		mockClock.Add(SOURCELEAP * time.Millisecond)
		common.Log.Debugf("Clock add to %d", common.GetNowInMilli())
		time.Sleep(1)
		for _, d := range datas {
			// Make sure time is going forward only
			if d[i].Timestamp > common.GetNowInMilli() {
				mockClock.Set(common.TimeFromUnixMilli(d[i].Timestamp))
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
		}
	}
	mockClock.Add(time.Duration(postleap) * time.Millisecond)
	common.Log.Debugf("Clock add to %d", common.GetNowInMilli())
	time.Sleep(1)
	// Check if stream done. Poll for metrics,
	/*
		for retry := 100; retry > 0; retry-- {
			time.Sleep(time.Duration(retry) * time.Millisecond)
			if err := compareMetrics(tp, metrics); err == nil {
				break
			} else {
				common.Log.Debugf("check metrics error at %d: %s", retry, err)
			}
		}
	*/
	return nil
}

func createStream(t *testing.T, tt ruleTest, j int, opt *api.RuleOption, sinkProps map[string]interface{}) ([][]*xsql.Tuple, int, *xstream.TopologyNew, *test.MockSink, <-chan error) {
	// Rest for each test
	cleanStateData()
	test.ResetClock(1541152485800)
	// Create stream
	var (
		sources    []*nodes.SourceNode
		datas      [][]*xsql.Tuple
		dataLength int
	)

	p := NewRuleProcessor(DbDir)
	parser := xsql.NewParser(strings.NewReader(tt.sql))
	if stmt, err := xsql.Language.Parse(parser); err != nil {
		t.Errorf("parse sql %s error: %s", tt.sql, err)
	} else {
		if selectStmt, ok := stmt.(*xsql.SelectStatement); !ok {
			t.Errorf("sql %s is not a select statement", tt.sql)
		} else {
			streams := xsql.GetStreams(selectStmt)
			for _, stream := range streams {
				data := testData[stream]
				dataLength = len(data)
				datas = append(datas, data)
				source := nodes.NewSourceNodeWithSource(stream, test.NewMockSource(data), map[string]string{
					"DATASOURCE": stream,
				})
				sources = append(sources, source)
			}
		}
	}
	tp, inputs, err := p.createTopoWithSources(&api.Rule{Id: fmt.Sprintf("%s_%d", tt.name, j), Sql: tt.sql, Options: opt}, sources)
	if err != nil {
		t.Error(err)
	}
	mockSink := test.NewMockSink()
	sink := nodes.NewSinkNodeWithSink("mockSink", mockSink, sinkProps)
	tp.AddSink(inputs, sink)
	errCh := tp.Open()
	return datas, dataLength, tp, mockSink, errCh
}

// Create or drop streams
func handleStream(createOrDrop bool, names []string, t *testing.T) {
	p := NewStreamProcessor(path.Join(DbDir, "stream"))
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
			default:
				t.Errorf("create stream %s fail", name)
			}
		} else {
			sql = `DROP STREAM ` + name
		}

		_, err := p.ExecStmt(sql)
		if err != nil {
			t.Log(err)
		}
	}
}
