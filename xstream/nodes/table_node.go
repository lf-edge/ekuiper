package nodes

import (
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
	"github.com/emqx/kuiper/xstream/extensions"
)

// Node for table source
type TableNode struct {
	*defaultNode
	sourceType string
	options    map[string]string
}

func NewTableNode(name string, options map[string]string) *TableNode {
	t, ok := options["TYPE"]
	if !ok {
		t = "file"
	}
	return &TableNode{
		sourceType: t,
		defaultNode: &defaultNode{
			name:        name,
			outputs:     make(map[string]chan<- interface{}),
			concurrency: 1,
		},
		options: options,
	}
}

func (m *TableNode) Open(ctx api.StreamContext, errCh chan<- error) {
	m.ctx = ctx
	logger := ctx.GetLogger()
	logger.Infof("open table node %s with option %v", m.name, m.options)
	go func() {
		props := getSourceConf(ctx, m.sourceType, m.options)
		//TODO apply properties like concurrency
		source, err := doGetTableSource(m.sourceType)
		if err != nil {
			m.drainError(errCh, err, ctx)
			return
		}
		err = source.Configure(m.options["DATASOURCE"], props)
		if err != nil {
			m.drainError(errCh, err, ctx)
			return
		}
		stats, err := NewStatManager("source", ctx)
		if err != nil {
			m.drainError(errCh, err, ctx)
			return
		}
		m.statManagers = append(m.statManagers, stats)
		stats.ProcessTimeStart()
		if data, err := source.Load(ctx); err != nil {
			stats.IncTotalExceptions()
			stats.ProcessTimeEnd()
			m.drainError(errCh, err, ctx)
			return
		} else {
			stats.IncTotalRecordsIn()
			stats.ProcessTimeEnd()
			logger.Debugf("table node %s is sending result", m.name)
			result := make([]*xsql.Tuple, len(data))
			for i, t := range data {
				tuple := &xsql.Tuple{Emitter: m.name, Message: t.Message(), Metadata: t.Meta(), Timestamp: common.GetNowInMilli()}
				result[i] = tuple
			}
			m.doBroadcast(result)
			stats.IncTotalRecordsOut()
			logger.Debugf("table node %s has consumed all data", m.name)
		}
	}()
}

func (m *TableNode) drainError(errCh chan<- error, err error, ctx api.StreamContext) {
	select {
	case errCh <- err:
	case <-ctx.Done():

	}
	return
}

func doGetTableSource(t string) (api.TableSource, error) {
	var s api.TableSource
	switch t {
	case "file":
		s = &extensions.FileSource{}
	default: //TODO table source plugin
		//s, err = plugins.GetTableSource(t)
		//if err != nil {
		//	return nil, err
		//}
	}
	return s, nil
}
