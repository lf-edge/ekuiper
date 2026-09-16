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

package file

import (
	"github.com/lf-edge/ekuiper/v2/internal/pkg/filex"
)

// The file path sandbox lives in filex so both the file connector and the
// RPC import/export paths share the same policy. These wrappers keep the
// connector code unchanged.

func externalFileAccessAllowed() bool {
	return filex.ExternalFileAccessAllowed()
}

func validateFilePath(p string) (string, error) {
	return filex.ValidateFilePath(p)
}

func validateFileName(name string) error {
	return filex.ValidateFileName(name)
}
