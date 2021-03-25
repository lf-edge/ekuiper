package nodes

import (
	"errors"
	"fmt"
	"github.com/emqx/kuiper/xsql"
	"github.com/emqx/kuiper/xstream/api"
)

/*
 *  This node will block the stream and buffer all the table tuples. Once buffered, it will combine the later input with the buffer
 *  The input for batch table MUST be *WindowTuples
 */
type JoinAlignNode struct {
	*defaultSinkNode
	statManager StatManager
	emitters    []string
	// states
	batch xsql.WindowTuplesSet
}

const StreamInputsKey = "$$streamInputs"

func NewJoinAlignNode(name string, emitters []string, options *api.RuleOption) (*JoinAlignNode, error) {
	n := &JoinAlignNode{
		emitters: emitters,
		batch:    make([]xsql.WindowTuples, len(emitters)),
	}
	n.defaultSinkNode = &defaultSinkNode{
		input: make(chan interface{}, options.BufferLength),
		defaultNode: &defaultNode{
			outputs:   make(map[string]chan<- interface{}),
			name:      name,
			sendError: options.SendError,
		},
	}
	return n, nil
}

func (n *JoinAlignNode) Exec(ctx api.StreamContext, errCh chan<- error) {
	n.ctx = ctx
	log := ctx.GetLogger()
	log.Debugf("JoinAlignNode %s is started", n.name)

	if len(n.outputs) <= 0 {
		go func() { errCh <- fmt.Errorf("no output channel found") }()
		return
	}
	stats, err := NewStatManager("op", ctx)
	if err != nil {
		go func() { errCh <- err }()
		return
	}
	n.statManager = stats
	var inputs []xsql.WindowTuplesSet
	batchLen := len(n.emitters)
	go func() {
		for {
			log.Debugf("JoinAlignNode %s is looping", n.name)
			select {
			// process incoming item
			case item, opened := <-n.input:
				processed := false
				if item, processed = n.preprocess(item); processed {
					break
				}
				n.statManager.IncTotalRecordsIn()
				n.statManager.ProcessTimeStart()
				if !opened {
					n.statManager.IncTotalExceptions()
					break
				}
				switch d := item.(type) {
				case error:
					n.Broadcast(d)
					n.statManager.IncTotalExceptions()
				case *xsql.Tuple:
					log.Debugf("JoinAlignNode receive tuple input %s", d)
					var temp xsql.WindowTuplesSet = make([]xsql.WindowTuples, 0)
					temp = temp.AddTuple(d)
					if batchLen == 0 {
						n.alignBatch(ctx, temp)
					} else {
						log.Debugf("JoinAlignNode buffer input")
						inputs = append(inputs, temp)
						ctx.PutState(StreamInputsKey, inputs)
						n.statManager.SetBufferLength(int64(len(n.input)))
					}
				case xsql.WindowTuplesSet:
					log.Debugf("JoinAlignNode receive window input %s", d)
					if batchLen == 0 {
						n.alignBatch(ctx, d)
					} else {
						log.Debugf("JoinAlignNode buffer input")
						inputs = append(inputs, d)
						ctx.PutState(StreamInputsKey, inputs)
						n.statManager.SetBufferLength(int64(len(n.input)))
					}
				case xsql.WindowTuples:
					log.Debugf("JoinAlignNode receive batch source %s", d)
					if batchLen <= 0 {
						errCh <- errors.New("Join receives too many table content")
					}
					n.batch[len(n.emitters)-batchLen] = d
					batchLen -= 1
					if batchLen == 0 {
						for _, w := range inputs {
							n.alignBatch(ctx, w)
						}
					}
				default:
					n.Broadcast(fmt.Errorf("run JoinAlignNode error: invalid input type but got %[1]T(%[1]v)", d))
					n.statManager.IncTotalExceptions()
				}
			case <-ctx.Done():
				log.Infoln("Cancelling join align node....")
				return
			}
		}
	}()
}

func (n *JoinAlignNode) alignBatch(ctx api.StreamContext, w xsql.WindowTuplesSet) {
	n.statManager.ProcessTimeStart()
	w = append(w, n.batch...)
	n.Broadcast(w)
	n.statManager.ProcessTimeEnd()
	n.statManager.IncTotalRecordsOut()
	n.statManager.SetBufferLength(int64(len(n.input)))
	ctx.PutState(StreamInputsKey, nil)
}

func (n *JoinAlignNode) GetMetrics() [][]interface{} {
	if n.statManager != nil {
		return [][]interface{}{
			n.statManager.GetMetrics(),
		}
	} else {
		return nil
	}
}
