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

//go:build (!no_base || sqlserver) && !no_sqlserver

package driver

import (
	// Microsoft SQL Server; the named import also registers the driver.
	mssql "github.com/denisenkom/go-mssqldb"
)

func init() {
	registerBindTransformer("sqlserver", mssqlStringTransformer)
	registerBindTransformer("mssql", mssqlStringTransformer)
}

// mssqlStringTransformer binds ordinary Go strings as VARCHAR, preserving
// the pre-parameterization non-Unicode literal behavior. Only the exact
// string type is converted; driver-specific values, valuers and named string
// types pass through untouched.
func mssqlStringTransformer(v any) any {
	if s, ok := v.(string); ok {
		return mssql.VarChar(s)
	}
	return v
}
