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

package mocknode

import (
	"encoding/base64"
	"os"
	"path"

	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/xsql"
)

// TestData The time diff must larger than timeleap
var TestData = map[string][]*xsql.Tuple{
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
				"color": "blue",
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
				"ts":    1541152486822,
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
				"size":  5,
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
				"self": Image,
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
				"name": "world",
			},
			Timestamp: 1541152486013,
		},
		{
			Emitter: "helloStr",
			Message: map[string]interface{}{
				"name": "golang",
			},
			Timestamp: 1541152487013,
		},
		{
			Emitter: "helloStr",
			Message: map[string]interface{}{
				"name": "peacock",
			},
			Timestamp: 1541152488013,
		},
	},
	"commands": {
		{
			Emitter: "commands",
			Message: map[string]interface{}{
				"cmd":          "get",
				"base64_img":   "my image",
				"encoded_json": "{\"name\": \"name1\",\"size\": 22}",
			},
			Timestamp: 1541152486013,
		},
		{
			Emitter: "commands",
			Message: map[string]interface{}{
				"cmd":          "detect",
				"base64_img":   "my image",
				"encoded_json": "{\"name\": \"name2\",\"size\": 33}",
			},
			Timestamp: 1541152487013,
		},
		{
			Emitter: "commands",
			Message: map[string]interface{}{
				"cmd":          "delete",
				"base64_img":   "my image",
				"encoded_json": "{\"name\": \"name3\",\"size\": 11}",
			},
			Timestamp: 1541152488013,
		},
	},
	"demoTable": {
		{
			Emitter: "demoTable",
			Message: map[string]interface{}{
				"ts":     1541152486013,
				"device": "device1",
			},
			Timestamp: 1541152486501,
		},
		{
			Emitter: "demoTable",
			Message: map[string]interface{}{
				"ts":     1541152486822,
				"device": "device2",
			},
			Timestamp: 1541152486502,
		},
		{
			Emitter: "demoTable",
			Message: map[string]interface{}{
				"ts":     1541152487632,
				"device": "device3",
			},
			Timestamp: 1541152488001,
		},
		{
			Emitter: "demoTable",
			Message: map[string]interface{}{
				"ts":     1541152488442,
				"device": "device4",
			},
			Timestamp: 1541152488002,
		},
		{
			Emitter: "demoTable",
			Message: map[string]interface{}{
				"ts":     1541152489252,
				"device": "device5",
			},
			Timestamp: 1541152488003,
		},
	},
	"shelves": {
		{
			Emitter: "shelves",
			Message: map[string]interface{}{
				"name": "name1",
				"size": 2,
				"shelf": map[string]interface{}{
					"id":       1541152486013,
					"theme":    "tandra",
					"subfield": "sub1",
				},
			},
			Timestamp: 1541152486501,
		},
		{
			Emitter: "shelves",
			Message: map[string]interface{}{
				"name": "name2",
				"size": 3,
				"shelf": map[string]interface{}{
					"id":       1541152486822,
					"theme":    "claro",
					"subfield": "sub2",
				},
			},
			Timestamp: 1541152486502,
		},
		{
			Emitter: "shelves",
			Message: map[string]interface{}{
				"name": "name3",
				"size": 4,
				"shelf": map[string]interface{}{
					"id":       1541152487632,
					"theme":    "dark",
					"subfield": "sub3",
				},
			},
			Timestamp: 1541152488001,
		},
	},
	"mes": {
		{
			Emitter: "mes",
			Message: map[string]interface{}{
				"message_id": "1541152486013",
				"text":       "message1",
			},
			Timestamp: 1541152486501,
		},
		{
			Emitter: "mes",
			Message: map[string]interface{}{
				"message_id": "1541152486501",
				"text":       "message2",
			},
			Timestamp: 1541152486501,
		},
		{
			Emitter: "mes",
			Message: map[string]interface{}{
				"message_id": "1541152487013",
				"text":       "message3",
			},
			Timestamp: 1541152487501,
		},
	},
	"demoArr": {
		{
			Emitter: "demoArr",
			Message: map[string]interface{}{
				"arr": []int{1, 2, 3, 4, 5},
				"x":   1,
				"y":   2,
				"arr2": []interface{}{
					map[string]interface{}{
						"a": 1,
						"b": 2,
					},
					map[string]interface{}{
						"a": 3,
						"b": 4,
					},
				},
				"a":    6,
				"arr3": []interface{}{1, 2, 3},
			},
			Timestamp: 1541152489253,
		},
	},
}

var Image, _ = getImg()

func getImg() ([]byte, string) {
	docsFolder, err := conf.GetLoc("docs/")
	if err != nil {
		conf.Log.Fatalf("Cannot find docs folder: %v", err)
	}
	image, err := os.ReadFile(path.Join(docsFolder, "cover.jpg"))
	if err != nil {
		conf.Log.Fatalf("Cannot read image: %v", err)
	}
	b64img := base64.StdEncoding.EncodeToString(image)
	return image, b64img
}
