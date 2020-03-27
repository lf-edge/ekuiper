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
	s := httptest.NewServer(
		http.FileServer(http.Dir("testzips")),
	)
	defer s.Close()
	endpoint := s.URL

	data := []struct {
		t   PluginType
		n   string
		u   string
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
			t: SINK,
			n: "file2",
			u: endpoint + "/sinks/file2.zip",
		}, {
			t: FUNCTION,
			n: "echo2",
			u: endpoint + "/functions/echo2.zip",
		}, {
			t:   FUNCTION,
			n:   "echo2",
			u:   endpoint + "/functions/echo2.zip",
			err: errors.New("invalid name echo2: duplicate"),
		},
	}
	manager, err := NewPluginManager()
	if err != nil {
		t.Error(err)
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(data))
	for i, tt := range data {
		err = manager.Register(tt.t, &Plugin{
			Name: tt.n,
			File: tt.u,
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
	data := []struct {
		t   PluginType
		n   string
		err error
	}{
		{
			t: SOURCE,
			n: "random2",
		}, {
			t: SINK,
			n: "file2",
		}, {
			t: FUNCTION,
			n: "echo2",
		},
	}
	manager, err := NewPluginManager()
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(data))

	for i, p := range data {
		err = manager.Delete(p.t, p.n)
		if err != nil {
			t.Errorf("%d: delete error : %s\n\n", i, err)
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
