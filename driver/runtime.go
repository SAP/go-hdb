/*
Copyright 2014 SAP SE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package driver

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
)

const goVersionPrefix = "go"

type goVersion struct {
	version, major, minor int
}

func newGoVersion(version string) *goVersion {

	if version[:2] != goVersionPrefix {
		panic(fmt.Sprintf("invalid go version: %s", version))
	}

	parts := strings.Split(version[2:], ".")
	if len(parts) != 3 {
		panic(fmt.Sprintf("invalid go version: %s", version))
	}

	v := new(goVersion)
	var err error

	if v.version, err = strconv.Atoi(parts[0]); err != nil {
		panic(fmt.Sprintf("invalid go version: %s", version))
	}
	if v.major, err = strconv.Atoi(parts[1]); err != nil {
		panic(fmt.Sprintf("invalid go version: %s", version))
	}
	if v.minor, err = strconv.Atoi(parts[2]); err != nil {
		panic(fmt.Sprintf("invalid go version: %s", version))
	}

	return v
}

func (v goVersion) greater(cmp *goVersion) bool {
	return v.version >= cmp.version && v.major >= cmp.major && v.minor >= cmp.minor
}

var goBuildVersion = newGoVersion(runtime.Version())

var go1_9_0 = &goVersion{version: 1, major: 9, minor: 0}

var minGo1_9 = goBuildVersion.greater(go1_9_0)
