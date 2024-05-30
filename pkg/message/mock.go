// Copyright 2024 EMQ Technologies Co., Ltd.
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

package message

import "github.com/lf-edge/ekuiper/contract/v2/api"

type MockPartialConverter struct {
	i int
}

func (m *MockPartialConverter) Encode(ctx api.StreamContext, d any) ([]byte, error) {
	// TODO implement me
	panic("implement me")
}

func (m *MockPartialConverter) Decode(ctx api.StreamContext, b []byte) (any, error) {
	// TODO implement me
	panic("implement me")
}

func (m *MockPartialConverter) DecodeField(ctx api.StreamContext, b []byte, f string) (any, error) {
	r := m.i % 2
	m.i++
	return r, nil
}
