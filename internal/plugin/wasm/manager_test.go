package wasm

import (
	"fmt"
	"os"
	"path"
	"testing"
)

func TestInitManager(t *testing.T) {
	InitManager()
	//etcDir:  /home/erfenjiao/ekuiper/etc
	//pluginDir:  /home/erfenjiao/ekuiper/plugins/wasm
	checkFileForFib("/home/erfenjiao/ekuiper/plugins/wasm", "/home/erfenjiao/ekuiper/etc", true)
}

func checkFileForFib(pluginDir, etcDir string, exist bool) error {
	fmt.Println("[test][checkFileForFib] start: ")
	requiredFiles := []string{
		//path.Join(pluginDir, "fibonacci", "fibonacci"),
		path.Join(pluginDir, "fibonacci", "fibonacci.json"),
		path.Join(etcDir, "functions", "fibonacci.json"),
	}
	for _, file := range requiredFiles {
		_, err := os.Stat(file)
		fmt.Println("[test][checkFileForFib] file: ", file)
		//file:  /home/erfenjiao/ekuiper/plugins/wasm/fibonacci/fibonacci
		if exist && err != nil {
			return err
		} else if !exist && err == nil {
			return fmt.Errorf("file still exists: %s", file)
		}
	}
	return nil
}
