package tskv

type Tskv interface {
	Set(k int64, v interface{}) (inserted bool, err error)
	Get(k int64, v interface{}) (found bool, err error)
	Last(v interface{}) (key int64, err error)
	Delete(k int64) error
	DeleteBefore(int64) error
	Close() error
}
