package testx

import (
	"github.com/emqx/kuiper/internal/conf"
)

// errstring returns the string representation of an error.
func Errstring(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

func GetDbDir() string {
	conf.InitConf()
	dbDir, err := conf.GetDataLoc()
	if err != nil {
		conf.Log.Fatal(err)
	}
	return dbDir
}
