package util

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/kv"
	"github.com/patrickmn/go-cache"
	"io/ioutil"
	"os"
	"path"
)

func migration(dir string) error {
	fpath := path.Join(dir, "stores.data")
	c := cache.New(cache.NoExpiration, 0)
	if err := gocacheOpen(c, fpath); nil != err {
		return err
	}
	defer gocacheClose(c, fpath)

	keys, err := gocacheKeys(c)
	if nil != err {
		return err
	}

	store := kv.GetDefaultKVStore(dir)
	if err := store.Open(); nil != err {
		return err
	}
	defer store.Close()

	for _, k := range keys {
		if value, ok := c.Get(k); !ok {
			return fmt.Errorf("not found %s from %s", k, fpath)
		} else {
			if err := store.Setnx(k, value); nil != err {
				return err
			}
			if err := gocacheDel(c, k); nil != err {
				return err
			}
		}
	}
	return os.Remove(fpath)
}

func DataMigration(dir string) error {
	var dirs []string
	dirs = append(dirs, dir)
	for i := 0; i < len(dirs); i++ {
		files, err := ioutil.ReadDir(dirs[i])
		if nil != err {
			return err
		}
		for _, file := range files {
			fname := file.Name()
			if file.IsDir() {
				dirs = append(dirs, path.Join(dirs[i], fname))
			} else if "stores.data" == fname {
				return migration(dirs[i])
			}
		}
	}
	return nil
}

func gocacheClose(c *cache.Cache, path string) error {
	if e := c.SaveFile(path); e != nil {
		return e
	}
	c.Flush()
	return nil
}

func gocacheOpen(c *cache.Cache, path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	}
	if e := c.LoadFile(path); e != nil {
		return e
	}
	return nil
}

func gocacheKeys(c *cache.Cache) (keys []string, err error) {
	if c == nil {
		return nil, fmt.Errorf("cache has not been initialized yet.")
	}
	its := c.Items()
	keys = make([]string, 0, len(its))
	for k := range its {
		keys = append(keys, k)
	}
	return keys, nil
}

func gocacheSet(c *cache.Cache, path, key string, value interface{}) error {
	if c == nil {
		return fmt.Errorf("cache has not been initialized yet.")
	}
	return c.Add(key, value, cache.NoExpiration)
}

func gocacheDel(c *cache.Cache, key string) error {
	if c == nil {
		return fmt.Errorf("cache has not been initialized yet.")
	}
	c.Delete(key)
	return nil
}
