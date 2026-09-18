package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/v2/pkg/errorx"

	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"
)

func TestRetryReconnectRetriesIOErrors(t *testing.T) {
	attempts := 0
	err := retryReconnect(context.Background(), backoff.NewConstantBackOff(0), func(context.Context) error {
		attempts++
		if attempts < 3 {
			return errorx.NewIOErr("database unavailable")
		}
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}

func TestRetryReconnectDoesNotRetryPermanentErrors(t *testing.T) {
	attempts := 0
	err := retryReconnect(context.Background(), backoff.NewConstantBackOff(0), func(context.Context) error {
		attempts++
		return errors.New("invalid database configuration")
	})

	require.EqualError(t, err, "invalid database configuration")
	require.Equal(t, 1, attempts)
}

func TestRetryReconnectStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	err := retryReconnect(ctx, backoff.NewConstantBackOff(time.Hour), func(context.Context) error {
		attempts++
		cancel()
		return errorx.NewIOErr("database unavailable")
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, attempts)
}
