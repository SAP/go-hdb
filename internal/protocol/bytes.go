/*
Copyright 2020 SAP SE

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

package protocol

func sizeBuffer(b []byte, size int) []byte {
	if b == nil || size > cap(b) {
		return make([]byte, size)
	}
	return b[:size]
}

func resizeBuffer(b1 []byte, size int) []byte {
	if b1 == nil || cap(b1) < size {
		b2 := make([]byte, size)
		copy(b2, b1) // !!! keep content
		return b2
	}
	return b1[:size]
}
