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

//go:build core
// +build core

package server

func pluginReset() {
}

func pluginExport() map[string]string {
	return nil
}

func pluginStatusExport() map[string]string {
	return nil
}

func pluginImport(plugins map[string]string) error {
	return nil
}

func portablePluginsReset() {
}

func portablePluginExport() map[string]string {
	return nil
}

func portablePluginStatusExport() map[string]string {
	return nil
}

func portablePluginImport(plugins map[string]string) {
}

func serviceReset() {
}

func serviceExport() map[string]string {
	return nil
}

func serviceStatusExport() map[string]string {
	return nil
}

func serviceImport(services map[string]string) {
}

func schemaReset() {
}

func schemaExport() map[string]string {
	return nil
}

func schemaStatusExport() map[string]string {
	return nil
}

func schemaImport(s map[string]string) error {
	return nil
}

func pluginPartialImport(plugins map[string]string) map[string]string {
	return nil
}

func schemaPartialImport(s map[string]string) map[string]string {
	return nil
}

func portablePluginPartialImport(plugins map[string]string) map[string]string {
	return nil
}

func servicePartialImport(services map[string]string) map[string]string {
	return serviceManager.ImportPartialServices(services)
}
