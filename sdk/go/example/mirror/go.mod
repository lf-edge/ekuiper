module github.com/lf-edge/ekuiper-plugin-mirror

require (
	github.com/lf-edge/ekuiper/sdk v0.0.0-20210916082120-031cd83a7fd8
	github.com/mitchellh/mapstructure v1.4.1
)

replace github.com/lf-edge/ekuiper/sdk => ../../

go 1.16
