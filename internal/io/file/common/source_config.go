package common

type FileSourceConfig struct {
	FileType         FileType `json:"fileType"`
	Path             string   `json:"path"`
	Interval         int      `json:"interval"`
	IsTable          bool     `json:"isTable"`
	Parallel         bool     `json:"parallel"`
	SendInterval     int      `json:"sendInterval"`
	ActionAfterRead  int      `json:"actionAfterRead"`
	MoveTo           string   `json:"moveTo"`
	HasHeader        bool     `json:"hasHeader"`
	Columns          []string `json:"columns"`
	IgnoreStartLines int      `json:"ignoreStartLines"`
	IgnoreEndLines   int      `json:"ignoreEndLines"`
	Delimiter        string   `json:"delimiter"`
	Decompression    string   `json:"decompression"`
}
