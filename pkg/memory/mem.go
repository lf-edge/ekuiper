// Copyright 2023-2025 EMQ Technologies Co., Ltd.
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

package memory

import (
	"github.com/shirou/gopsutil/v3/mem"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/cgroup"
)

var MemoryTotal uint64

func init() {
	if cgroup.InContainer() {
		m1, err := cgroup.MemTotalCGroup()
		if err != nil {
			conf.Log.Warnf("get total memory failed, err:%v", err)
		} else {
			conf.Log.Infof("get cgroup total memory %v success", m1)
			MemoryTotal = m1
		}
	} else {
		m2, err := mem.VirtualMemory()
		if err != nil {
			conf.Log.Warnf("get total memory failed, err:%v", err)
		} else {
			conf.Log.Infof("set server total memory %v success", m2.Total)
			MemoryTotal = m2.Total
		}
	}
}

func GetMemoryTotal() uint64 {
	return MemoryTotal
}
