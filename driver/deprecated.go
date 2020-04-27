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

package driver

import (
	"database/sql/driver"
)

// deprecated driver interface methods
func (*conn) Prepare(query string) (driver.Stmt, error)                     { panic("deprecated") }
func (*conn) Begin() (driver.Tx, error)                                     { panic("deprecated") }
func (*conn) Exec(query string, args []driver.Value) (driver.Result, error) { panic("deprecated") }
func (*conn) Query(query string, args []driver.Value) (driver.Rows, error)  { panic("deprecated") }
func (*stmt) Exec(args []driver.Value) (driver.Result, error)               { panic("deprecated") }
func (*stmt) Query(args []driver.Value) (rows driver.Rows, err error)       { panic("deprecated") }

// replaced driver interface methods
// sql.Stmt.ColumnConverter --> replaced by CheckNamedValue
