package catalog

type Catalog struct {
	Streams      map[string]*Stream
	ScanTables   map[string]*ScanTable
	LookUpTables map[string]*LookUpTable
}

func (c *Catalog) GetStream(streamName string) (*Stream, bool) {
	s, ok := c.Streams[streamName]
	return s, ok
}

type ScanTable struct {
}

type LookUpTable struct {
}
