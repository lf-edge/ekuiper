// Copyright 2022 EMQ Technologies Co., Ltd.
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

package graph

type Function struct {
	Expr string `json:"expr"`
}

type Filter struct {
	Expr string `json:"expr"`
}

type Select struct {
	Fields []string `json:"fields"`
}

type Window struct {
	Type     string `json:"type"`
	Unit     string `json:"unit"`
	Size     int    `json:"size"`
	Interval int    `json:"interval"`
}

type Join struct {
	From  string `json:"from"`
	Joins []struct {
		Name string `json:"name"`
		Type string `json:"type"`
		On   string `json:"on"`
	}
}

type Groupby struct {
	Dimensions []string `json:"dimensions"`
}

type Orderby struct {
	Sorts []struct {
		Field string `json:"field"`
		Desc  bool   `json:"desc"`
	}
}

type Switch struct {
	Cases            []string `json:"cases"`
	StopAtFirstMatch bool     `json:"stopAtFirstMatch"`
}
