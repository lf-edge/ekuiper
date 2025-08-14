package node

import (
	context "context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/v2/api"
)

func TestOrderNodeMessageHandler(t *testing.T) {
	handler := NewOrderNodeMessageHandler(context.Background(), 4, func(ctx context.Context, data *NodeMessage) *NodeMessage {
		x := rand.Intn(100)
		time.Sleep(time.Duration(x) * time.Millisecond)
		return data
	})
	msgs := make([]*NodeMessage, 0)
	for i := 0; i < 100; i++ {
		m := map[string]interface{}{
			"id": int64(i),
		}
		tuple, err := api.NewTupleFromData("mock", m)
		require.NoError(t, err)
		msgs = append(msgs, &NodeMessage{
			Tuples: []*api.Tuple{
				tuple,
			},
		})
	}
	for _, msg := range msgs {
		handler.In <- msg
	}
	for i := 0; i < 100; i++ {
		m := map[string]interface{}{
			"id": int64(i),
		}
		select {
		case msg := <-handler.Out:
			require.Equal(t, m, msg.Tuples[0].ToMap())
			fmt.Println(fmt.Sprintf("assert %v msg success", i))
		}
	}
	fmt.Println("assert close start")
	handler.GraceClose()
	fmt.Println("assert close finish")
}
