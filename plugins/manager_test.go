package plugins

import (
	"errors"
	"fmt"
	"os"
	"path"
	"reflect"
	"testing"
)

func TestManager_Register(t *testing.T) {
	const endpoint = "http://127.0.0.1/plugins"
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
			t: SOURCE,
			n: "random2",
			u: endpoint + "/sources/random2.zip",
		},
	}
	manager, err := NewPluginManager()
	if err != nil {
		t.Error(err)
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(data))
	for i, tt := range data {
		err = manager.Register(tt.t, tt.n, tt.u, func() {})
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
		},
	}
	manager, err := NewPluginManager()
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(data))
	if len(data) > 3 && manager.pluginDir == "" {
		t.Errorf("Expect %s, actual %s; error is %s. \n", "test", "3", "3")
	}
	for i, p := range data {
		err = manager.Delete(p.t, p.n, func() {})
		if err != nil {
			t.Errorf("%d: delete error : %s\n\n", i, err)
		}
	}
}

func checkFile(pluginDir string, etcDir string, t PluginType, name string) error {
	soPath := path.Join(pluginDir, pluginFolders[t], ucFirst(name)+".so")
	_, err := os.Stat(soPath)
	if err != nil {
		return err
	}
	etcPath := path.Join(etcDir, pluginFolders[t], name+".yaml")
	_, err = os.Stat(etcPath)
	if err != nil {
		return err
	}
	return nil
}
