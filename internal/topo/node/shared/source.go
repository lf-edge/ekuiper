package shared

import (
	"github.com/lf-edge/ekuiper/pkg/api"
)

type source struct {
	sink  string
	id    string
	input chan map[string]interface{}
}

func (s *source) Open(ctx api.StreamContext, consumer chan<- api.SourceTuple, errCh chan<- error) {
	s.id = ctx.GetOpId()
	ch, err := getOrCreateSinkConsumerChannel(s.sink, s.id)
	if err != nil {
		errCh <- err
		return
	}
	s.input = ch
	for {
		select {
		case v, opened := <-s.input:
			if !opened {
				return
			}
			consumer <- api.NewDefaultSourceTuple(v, make(map[string]interface{}))
		case <-ctx.Done():
			return
		}
	}
}

func (s *source) Configure(datasource string, props map[string]interface{}) error {
	s.sink = datasource
	return nil
}

func (s *source) Close(ctx api.StreamContext) error {
	return closeSourceConsumerChannel(s.sink, s.id)
}
