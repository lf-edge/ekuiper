package plugins

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"reflect"
	"testing"
)

func TestManager_Register(t *testing.T) {
	//file server
	s := httptest.NewServer(
		http.FileServer(http.Dir("testzips")),
	)
	defer s.Close()
	endpoint := s.URL
	//callback server
	h := http.NewServeMux()
	h.HandleFunc("/callback/", func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(http.StatusOK)
	})
	h.HandleFunc("/callbackE/", func(res http.ResponseWriter, req *http.Request) {
		http.Error(res, "error", 500)
	})
	hs := httptest.NewServer(h)
	defer hs.Close()

	data := []struct {
		t   PluginType
		n   string
		u   string
		c   string
		err error
	}{
		{
			t:   SOURCE,
			n:   "",
			u:   "",
			err: errors.New("invalid name : should not be empty"),
		}, {
			t:   SOURCE,
			n:   "zipMissConf",
			u:   endpoint + "/sources/zipMissConf.zip",
			err: errors.New("fail to unzip file " + endpoint + "/sources/zipMissConf.zip: invalid zip file: so file or conf file is missing"),
		}, {
			t:   SINK,
			n:   "urlerror",
			u:   endpoint + "/sinks/nozip",
			err: errors.New("invalid uri " + endpoint + "/sinks/nozip"),
		}, {
			t:   SINK,
			n:   "zipWrongname",
			u:   endpoint + "/sinks/zipWrongName.zip",
			err: errors.New("fail to unzip file " + endpoint + "/sinks/zipWrongName.zip: invalid zip file: so file or conf file is missing"),
		}, {
			t:   FUNCTION,
			n:   "zipMissSo",
			u:   endpoint + "/functions/zipMissSo.zip",
			err: errors.New("fail to unzip file " + endpoint + "/functions/zipMissSo.zip: invalid zip file: so file or conf file is missing"),
		}, {
			t: SOURCE,
			n: "random2",
			u: endpoint + "/sources/random2.zip",
		}, {
			t: SOURCE,
			n: "random3",
			u: endpoint + "/sources/random3.zip",
			c: hs.URL + "/callback",
		}, {
			t: SINK,
			n: "file2",
			u: endpoint + "/sinks/file2.zip",
		}, {
			t:   SINK,
			n:   "file3",
			u:   endpoint + "/sinks/file3.zip",
			c:   hs.URL + "/callbackE",
			err: errors.New("action succeeded but callback failed: status 500 Internal Server Error"),
		}, {
			t: FUNCTION,
			n: "echo2",
			u: endpoint + "/functions/echo2.zip",
		}, {
			t:   FUNCTION,
			n:   "echo2",
			u:   endpoint + "/functions/echo2.zip",
			err: errors.New("invalid name echo2: duplicate"),
		}, {
			t:   FUNCTION,
			n:   "echo3",
			u:   endpoint + "/functions/echo3.zip",
			c:   hs.URL + "/nonExist",
			err: errors.New("action succeeded but callback failed: status 404 Not Found"),
		},
	}
	manager, err := NewPluginManager()
	if err != nil {
		t.Error(err)
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(data))
	for i, tt := range data {
		err = manager.Register(tt.t, &Plugin{
			Name:     tt.n,
			File:     tt.u,
			Callback: tt.c,
		})
		if !reflect.DeepEqual(tt.err, err) {
			t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.err, err)
		} else if tt.err == nil {
			err := checkFile(manager.pluginDir, manager.etcDir, tt.t, tt.n)
			if err != nil {
				t.Errorf("%d: error : %s\n\n", i, err)
			}
		}
	}

}

func TestManager_Delete(t *testing.T) {
	h := http.NewServeMux()
	h.HandleFunc("/callback/", func(res http.ResponseWriter, req *http.Request) {
		res.WriteHeader(http.StatusOK)
	})
	h.HandleFunc("/callbackE/", func(res http.ResponseWriter, req *http.Request) {
		http.Error(res, "error", 500)
	})
	s := httptest.NewServer(h)
	defer s.Close()
	data := []struct {
		t   PluginType
		n   string
		c   string
		err error
	}{
		{
			t:   SOURCE,
			n:   "random2",
			c:   s.URL + "/callbackN",
			err: errors.New("action succeeded but callback failed: status 404 Not Found"),
		}, {
			t: SINK,
			n: "file2",
			c: s.URL + "/callback",
		}, {
			t:   FUNCTION,
			n:   "echo2",
			c:   s.URL + "/callbackE",
			err: errors.New("action succeeded but callback failed: status 500 Internal Server Error"),
		}, {
			t: SOURCE,
			n: "random3",
		}, {
			t: SINK,
			n: "file3",
		}, {
			t: FUNCTION,
			n: "echo3",
		},
	}
	manager, err := NewPluginManager()
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(data))

	for i, p := range data {
		err = manager.Delete(p.t, p.n, p.c)
		if !reflect.DeepEqual(p.err, err) {
			t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, p.err, err)
		}
	}
}

func checkFile(pluginDir string, etcDir string, t PluginType, name string) error {
	soPath := path.Join(pluginDir, PluginTypes[t], ucFirst(name)+".so")
	_, err := os.Stat(soPath)
	if err != nil {
		return err
	}
	if t == SOURCE {
		etcPath := path.Join(etcDir, PluginTypes[t], name+".yaml")
		_, err = os.Stat(etcPath)
		if err != nil {
			return err
		}
	}
	return nil
}
