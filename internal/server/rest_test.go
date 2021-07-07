// Copyright 2021 EMQ Technologies Co., Ltd.
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

package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestParseHtml(t1 *testing.T) {
	var tests = []struct {
		html    string
		plugins []string
		arch    string
		error   string
	}{
		{
			html: `<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 3.2 Final//EN"><html>
			<title>Directory listing for enterprise: /4.1.1/</title>
			<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
			<meta name="robots" content="noindex,nofollow">
			<body>
			<h2>Directory listing for enterprise: /4.1.1/</h2>
			<hr>
			<ul>
				<li><a href="file_386.zip">file_386.zip</a>
				<li><a href="file_amd64.zip">file_amd64.zip</a>
				<li><a href="file_arm.zip">file_arm.zip</a>
				<li><a href="file_arm64.zip">file_arm64.zip</a>
				<li><a href="file_ppc64le.zip">file_ppc64le.zip</a>

				<li><a href="influx_386.zip">influx_386.zip</a>
				<li><a href="influx_amd64.zip">influx_amd64.zip</a>
				<li><a href="influx_arm.zip">influx_arm.zip</a>
				<li><a href="influx_arm64.zip">influx_arm64.zip</a>
				<li><a href="influx_ppc64le.zip">influx_ppc64le.zip</a>
			</ul>
			<hr>
			</body>
			</html>
			`,
			arch:    "arm64",
			plugins: []string{"file", "influx"},
			error:   "",
		},

		{
			html: `<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 3.2 Final//EN"><html>
			<title>Directory listing for enterprise: /4.1.1/</title>
			<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
			<meta name="robots" content="noindex,nofollow">
			<body>
			<h2>Directory listing for enterprise: /4.1.1/</h2>
			<hr>
			<ul>
				<li><a href="file_386.zip">file_386.zip</a>
				<li><a href="file_amd64.zip">file_amd64.zip</a>
				<li><a href="file_arm.zip">file_arm.zip</a>
				<li><a href="file_arm64.zip">file_arm64.zip</a>
				<li><a href="file_ppc64le.zip">file_ppc64le.zip</a>

				<li><a href="influx_386.zip">influx_386.zip</a>
				<li><a href="influx_amd64.zip">influx_amd64.zip</a>
				<li><a href="influx_arm.zip">influx_arm.zip</a>
				<li><a href="influx_arm64.zip">influx_arm64.zip</a>
				<li><a href="influx_ppc64le.zip">influx_ppc64le.zip</a>
			</ul>
			<hr>
			</body>
			</html>
			`,
			arch:    "arm7",
			plugins: []string{},
			error:   "",
		},

		{
			html: `<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 3.2 Final//EN"><html>
			<title>Directory listing for enterprise: /4.1.1/</title>
			<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
			<meta name="robots" content="noindex,nofollow">
			<body>
			<h2>Directory listing for enterprise: /4.1.1/</h2>
			<hr>
			<ul>
				<li><a href="file_386.zip">file_386.zip</a>
				<li><a href="file_amd64.zip">file_amd64.zip</a>
				<li><a href="file_arm.zip">file_arm.zip</a>
				<li><a href="file_arm64.zip">file_arm64.zip</a>
				<li><a href="file_ppc64le.zip">file_ppc64le.zip</a>

				<li><a href="influx_arm.zip">influx_arm.zip</a>
				<li><a href="influx_arm64.zip">influx_arm64.zip</a>
				<li><a href="influx_ppc64le.zip">influx_ppc64le.zip</a>
			</ul>
			<hr>
			</body>
			</html>
			`,
			arch:    "amd64",
			plugins: []string{"file"},
			error:   "",
		},

		{
			html: `<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 3.2 Final//EN"><html>
			<title>Directory listing for enterprise: /4.1.1/</title>
			<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
			<meta name="robots" content="noindex,nofollow">
			<body>
			<h2>Directory listing for enterprise: /4.1.1/</h2>
			<hr>
			<ul>
				
			</ul>
			<hr>
			</body>
			</html>
			`,
			arch:    "amd64",
			plugins: []string{},
			error:   "",
		},

		{
			html:    ``,
			arch:    "amd64",
			plugins: []string{},
			error:   "",
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(tests))
	for i, t := range tests {
		result := extractFromHtml(t.html, t.arch)
		if t.error == "" && !reflect.DeepEqual(t.plugins, result) {
			t1.Errorf("%d. %q\n\nresult mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", i, t.html, t.plugins, result)
		}
	}
}

func TestFetchPluginList(t1 *testing.T) {
	version = "0.9.1"
	// Start a local HTTP server
	server1 := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Send response to be tested
		if _, err := rw.Write([]byte(`<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 3.2 Final//EN"><html>
			<title>Directory listing for enterprise: /4.1.1/</title>
			<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
			<meta name="robots" content="noindex,nofollow">
			<body>
			<h2>Directory listing for enterprise: /4.1.1/</h2>
			<hr>
			<ul>
				<li><a href="file_386.zip">file_386.zip</a>
				<li><a href="file_amd64.zip">file_amd64.zip</a>
				<li><a href="file_arm.zip">file_arm.zip</a>
				<li><a href="file_arm64.zip">file_arm64.zip</a>
				<li><a href="file_ppc64le.zip">file_ppc64le.zip</a>

				<li><a href="influx_386.zip">influx_386.zip</a>
				<li><a href="influx_amd64.zip">influx_amd64.zip</a>
				<li><a href="influx_arm.zip">influx_arm.zip</a>
				<li><a href="influx_arm64.zip">influx_arm64.zip</a>
				<li><a href="influx_ppc64le.zip">influx_ppc64le.zip</a>
			</ul>
			<hr>
			</body>
			</html>
			`)); err != nil {
			fmt.Printf("%s", err)
		}

	}))

	server2 := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		// Send response to be tested
		if _, err := rw.Write([]byte(`<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 3.2 Final//EN"><html>
			<title>Directory listing for enterprise: /4.1.1/</title>
			<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
			<meta name="robots" content="noindex,nofollow">
			<body>
			<h2>Directory listing for enterprise: /4.1.1/</h2>
			<hr>
			<ul>
				<li><a href="file_arm64.zip">file_arm64.zip</a>

				<li><a href="zmq_386.zip">influx_386.zip</a>
				<li><a href="zmq_amd64.zip">influx_amd64.zip</a>
				<li><a href="zmq_arm.zip">influx_arm.zip</a>
				<li><a href="zmq_arm64.zip">influx_arm64.zip</a>
				<li><a href="zmq_ppc64le.zip">influx_ppc64le.zip</a>
			</ul>
			<hr>
			</body>
			</html>
			`)); err != nil {
			fmt.Printf("%s", err)
		}

	}))

	// Close the server when test finishes
	defer server2.Close()

	if e, r := fetchPluginList(strings.Join([]string{server1.URL, server2.URL}, ","), "sinks", "alpine", "arm64"); e != nil {
		t1.Errorf("Error: %v", e)
	} else {
		exp := map[string]string{
			"file":   server1.URL + "/kuiper-plugins/" + version + "/alpine/sinks/file_arm64.zip",
			"influx": server1.URL + "/kuiper-plugins/" + version + "/alpine/sinks/influx_arm64.zip",
			"zmq":    server2.URL + "/kuiper-plugins/" + version + "/alpine/sinks/zmq_arm64.zip",
		}
		if !reflect.DeepEqual(exp, r) {
			t1.Errorf("result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", exp, r)
		}
	}

	if e, r := fetchPluginList(strings.Join([]string{server2.URL}, ","), "sinks", "alpine", "arm64"); e != nil {
		t1.Errorf("Error: %v", e)
	} else {
		exp := map[string]string{
			"zmq":  server2.URL + "/kuiper-plugins/" + version + "/alpine/sinks/zmq_arm64.zip",
			"file": server2.URL + "/kuiper-plugins/" + version + "/alpine/sinks/file_arm64.zip",
		}
		if !reflect.DeepEqual(exp, r) {
			t1.Errorf("result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", exp, r)
		}
	}

	if e, r := fetchPluginList(strings.Join([]string{server1.URL, server2.URL}, ","), "sinks", "alpine", "armv7"); e != nil {
		t1.Errorf("Error: %v", e)
	} else {
		exp := map[string]string{}
		if !reflect.DeepEqual(exp, r) {
			t1.Errorf("result mismatch:\n\nexp=%#v\n\ngot=%#v\n\n", exp, r)
		}
	}
}
