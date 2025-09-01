package nexmark

import (
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
)

type NexmarkSource struct {
}

func (n NexmarkSource) Provision(ctx api.StreamContext, configs map[string]any) error {
	//TODO implement me
	panic("implement me")
}

func (n NexmarkSource) Close(ctx api.StreamContext) error {
	//TODO implement me
	panic("implement me")
}

func (n NexmarkSource) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	//TODO implement me
	panic("implement me")
}

func (n NexmarkSource) Pull(ctx api.StreamContext, trigger time.Time, ingest api.TupleIngest, ingestError api.ErrorIngest) {
	//TODO implement me
	panic("implement me")
}
