// Copyright 2026 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package urlencoded

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/message"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestWriter(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "op1")
	w, err := NewWriter(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, w.New(ctx))
	require.NoError(t, w.Write(ctx, map[string]any{"a": 1, "b": "first"}))
	require.NoError(t, w.Write(ctx, map[string]any{"a": 2, "b": "second"}))
	result, err := w.Flush(ctx)
	require.NoError(t, err)
	require.Equal(t, "a=1&b=first&a=2&b=second", string(result))
}

func TestWriterRaw(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "op1")
	w, err := NewWriter(ctx, nil)
	require.NoError(t, err)
	rawWriter := w.(message.RawConvertWriter)
	require.NoError(t, w.New(ctx))
	require.NoError(t, rawWriter.WriteRaw(ctx, []byte("a=1")))
	require.NoError(t, rawWriter.WriteRaw(ctx, []byte("b=two+words")))
	result, err := w.Flush(ctx)
	require.NoError(t, err)
	require.Equal(t, "a=1&b=two+words", string(result))
}

func TestWriterFlushOwnership(t *testing.T) {
	ctx := mockContext.NewMockContext("test", "op1")
	w, err := NewWriter(ctx, nil)
	require.NoError(t, err)
	rawWriter := w.(message.RawConvertWriter)
	require.NoError(t, w.New(ctx))
	require.NoError(t, rawWriter.WriteRaw(ctx, []byte("a=first")))
	first, err := w.Flush(ctx)
	require.NoError(t, err)
	firstSnapshot := append([]byte(nil), first...)

	require.NoError(t, w.New(ctx))
	require.NoError(t, rawWriter.WriteRaw(ctx, []byte("a=later")))
	_, err = w.Flush(ctx)
	require.NoError(t, err)
	require.Equal(t, firstSnapshot, first)
}
