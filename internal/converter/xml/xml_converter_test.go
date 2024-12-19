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

package xml

import (
	"testing"

	"github.com/stretchr/testify/require"

	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func TestDecodeEncodeXML(t *testing.T) {
	converter := NewXMLConverter()
	ctx := mockContext.NewMockContext("1", "2")
	tests := []struct {
		name       string
		xmlData    string
		expected   map[string]interface{}
		encodeData string
	}{
		{
			name:    "Single XML1",
			xmlData: `<dcterms:license rdf:resource="license"/>`,
			expected: map[string]interface{}{
				"dcterms:license": map[string]interface{}{
					"rdf:resource": "license",
				},
			},
			encodeData: `<license xmlns="dcterms" xmlns:rdf="rdf" rdf:resource="license"></license>`,
		},
		{
			name:    "Simple XML1",
			xmlData: `<bookstore rdf:resource="license">7.99</bookstore>`,
			expected: map[string]interface{}{
				"bookstore": map[string]interface{}{
					"@value":       7.99,
					"rdf:resource": "license",
				},
			},
			encodeData: `<bookstore xmlns:rdf="rdf" rdf:resource="license">7.99</bookstore>`,
		},
		{
			name:    "Simple XML1",
			xmlData: `<bookstore rdf:resource="license"><price>7.99</price></bookstore>`,
			expected: map[string]interface{}{
				"bookstore": map[string]interface{}{
					"@value": []interface{}{
						map[string]interface{}{
							"price": map[string]interface{}{
								"@value": 7.99,
							},
						},
					},
					"rdf:resource": "license",
				},
			},
			encodeData: `<bookstore xmlns:rdf="rdf" rdf:resource="license"><price>7.99</price></bookstore>`,
		},
		{
			name:    "Simple XML2",
			xmlData: `<bookstore><price>7.99</price><price>8.99</price></bookstore>`,
			expected: map[string]interface{}{
				"bookstore": map[string]interface{}{
					"@value": []interface{}{
						map[string]interface{}{
							"price": map[string]interface{}{
								"@value": 7.99,
							},
						},
						map[string]interface{}{
							"price": map[string]interface{}{
								"@value": 8.99,
							},
						},
					},
				},
			},
			encodeData: `<bookstore><price>7.99</price><price>8.99</price></bookstore>`,
		},

		{
			name:    "Simple XML4",
			xmlData: `<bookstore><book><year>1995</year><price>10.99</price><ok>true</ok><author>Tom</author></book></bookstore>`,
			expected: map[string]interface{}{
				"bookstore": map[string]interface{}{
					"@value": []interface{}{
						map[string]interface{}{
							"book": map[string]interface{}{
								"@value": []interface{}{
									map[string]interface{}{
										"year": map[string]interface{}{
											"@value": int64(1995),
										},
									},
									map[string]interface{}{
										"price": map[string]interface{}{
											"@value": 10.99,
										},
									},
									map[string]interface{}{
										"ok": map[string]interface{}{
											"@value": true,
										},
									},
									map[string]interface{}{
										"author": map[string]interface{}{
											"@value": "Tom",
										},
									},
								},
							},
						},
					},
				},
			},
			encodeData: `<bookstore><book><year>1995</year><price>10.99</price><ok>true</ok><author>Tom</author></book></bookstore>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := converter.Decode(ctx, []byte(tt.xmlData))
			require.NoError(t, err)
			require.Equal(t, tt.expected, got)
			bs, err := converter.Encode(ctx, got)
			require.NoError(t, err)
			require.Equal(t, tt.encodeData, string(bs))
		})
	}
}

func TestInvalidXmlData(t *testing.T) {
	converter := NewXMLConverter()
	ctx := mockContext.NewMockContext("1", "2")
	_, err := converter.Decode(ctx, []byte("123"))
	require.Error(t, err)
}
