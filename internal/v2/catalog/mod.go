package catalog

import "sync"

type Catalog struct {
	sync.RWMutex
	Streams      map[string]*Stream
	ScanTables   map[string]*ScanTable
	LookUpTables map[string]*LookUpTable
}

func NewCatalog() *Catalog {
	return &Catalog{
		Streams:      make(map[string]*Stream),
		ScanTables:   make(map[string]*ScanTable),
		LookUpTables: make(map[string]*LookUpTable),
	}
}

func (c *Catalog) AddStream(streamName string, s *Stream) {
	c.Lock()
	defer c.Unlock()
	c.Streams[streamName] = s
}

func (c *Catalog) GetStream(streamName string) (*Stream, bool) {
	c.RLock()
	defer c.RUnlock()
	s, ok := c.Streams[streamName]
	return s, ok
}

type ScanTable struct {
}

type LookUpTable struct {
}
