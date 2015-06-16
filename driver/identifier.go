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
	"regexp"
	"strconv"
	"strings"
)

var reSimple = regexp.MustCompile("^[_A-Z][_#$A-Z0-9]*$")

// Identifier in hdb SQL statements like schema or table name.
type Identifier string

// String implements Stringer interface.
func (i Identifier) String() string {
	s := string(i)
	if reSimple.MatchString(s) {
		return s
	}
	return strconv.Quote(s)
}

// SplitIdentifier splits a string by the identifier separator "." into its Identifier components.
func SplitIdentifier(s string) []Identifier {
	inQuotes := false
	f := func(c rune) bool {
		switch {
		case c == '"':
			inQuotes = !inQuotes
			return false
		case inQuotes:
			return false
		default:
			return c == '.'
		}
	}

	a := strings.FieldsFunc(s, f)
	ids := make([]Identifier, len(a))
	for i, s := range a {
		if t, err := strconv.Unquote(s); err != nil { //no quotes found
			ids[i] = Identifier(s)
		} else {
			ids[i] = Identifier(t)
		}
	}
	return ids
}

// JoinIdentifier joins an array of Identifiers by the identifier separator "." to a string.
func JoinIdentifier(a []Identifier) string {
	ids := make([]string, len(a))
	for i, id := range a {
		ids[i] = id.String()
	}
	return strings.Join(ids, ".")
}
