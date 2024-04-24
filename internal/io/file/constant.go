// Copyright 2023 EMQ Technologies Co., Ltd.
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

package file

type FileType string

const (
	JSON_TYPE  FileType = "json"
	CSV_TYPE   FileType = "csv"
	LINES_TYPE FileType = "lines"
)

const (
	GZIP = "gzip"
	ZSTD = "zstd"
)

var fileTypes = map[FileType]struct{}{
	JSON_TYPE:  {},
	CSV_TYPE:   {},
	LINES_TYPE: {},
}

var compressionTypes = map[string]struct{}{
	GZIP: {},
	ZSTD: {},
}
