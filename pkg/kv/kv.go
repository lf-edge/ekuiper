package kv

type KeyValue interface {
	Open() error
	Close() error
	// Set key to hold string value if key does not exist otherwise return an error
	Setnx(key string, value interface{}) error
	// Set key to hold the string value. If key already holds a value, it is overwritten
	Set(key string, value interface{}) error
	Get(key string, val interface{}) (bool, error)
	//Must return *common.Error with NOT_FOUND error
	Delete(key string) error
	Keys() (keys []string, err error)
	Clean() error
}

func GetDefaultKVStore(fpath string) (ret KeyValue) {
	return GetSqliteKVStore(fpath)
}
