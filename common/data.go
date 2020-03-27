package common

type RuleDesc struct {
	Name, Json string
}

type PluginDesc struct {
	RuleDesc
	Type    int
	Restart bool
}
