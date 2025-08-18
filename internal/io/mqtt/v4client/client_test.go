package v4client

import (
	"testing"

	pahoMqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/testx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestMultiTopicSubscribe(t *testing.T) {
	url, serverCancel, err := testx.InitBroker("TestMultiTopicSubscribe")
	require.NoError(t, err)
	defer func() {
		serverCancel()
	}()
	ctx, _ := mockContext.NewMockContext("ruleEof", "op1").WithCancel()
	c, err := Provision(ctx, map[string]any{
		"server":     url,
		"datasource": "test1,test2",
		"qos":        0,
	}, func(ctx api.StreamContext) {

	}, func(ctx api.StreamContext, e error) {

	}, func(ctx api.StreamContext) {

	})
	require.NoError(t, err)
	require.NoError(t, c.Connect(ctx))
	// Create a channel to receive the result
	resultCh := make(chan any, 10)
	require.NoError(t, c.Subscribe(ctx, "topic1,topic2", 0, func(ctx api.StreamContext, msg any) {
		resultCh <- msg
	}))
	require.NoError(t, c.Publish(ctx, "topic1", 0, false, []byte{41}, nil))
	require.NoError(t, c.Publish(ctx, "topic2", 0, false, []byte{42}, nil))
	v1 := <-resultCh
	m1, ok := v1.(pahoMqtt.Message)
	require.True(t, ok)
	require.Equal(t, m1.Payload(), []byte{41})
	v2 := <-resultCh
	m2, ok := v2.(pahoMqtt.Message)
	require.True(t, ok)
	require.Equal(t, m2.Payload(), []byte{42})
	require.NoError(t, c.Unsubscribe(ctx, "test1,test2"))
}
