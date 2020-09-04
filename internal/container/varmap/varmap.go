// SPDX-FileCopyrightText: 2014-2020 SAP SE
//
// SPDX-License-Identifier: Apache-2.0

// Package varmap implements a key value map like used in session variables.
package varmap

import (
	"sync"
	"sync/atomic"
)

// A VarMap is a simple map[string]string keeping track of added, changed and deleted items.
// It is safe for concurrent use by multiple goroutines.
type VarMap struct {
	updated int32 // atomic access

	mu sync.Mutex
	m  map[string]string
	c  map[string]bool // track changes: entry exists -> updated / value true -> deleted
}

// NewVarMap returns a new VarMap.
func NewVarMap() *VarMap {
	return &VarMap{
		m: make(map[string]string),
		c: make(map[string]bool),
	}
}

func (vm *VarMap) deleteAll() {
	for k := range vm.m {
		vm.c[k] = true
	}
	vm.m = make(map[string]string)
}

// HasUpdates returns true if the VarMap has changed since the last call of Delta.
func (vm *VarMap) HasUpdates() bool {
	return atomic.LoadInt32(&vm.updated) == 1
}

// StoreMap sores a string key value map in VarMap.
func (vm *VarMap) StoreMap(m map[string]string) {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	vm.deleteAll()
	for k, v := range m {
		vm.m[k] = v
		vm.c[k] = false
	}
	atomic.StoreInt32(&vm.updated, 1)
}

// LoadMap returns the content of a VarMap as string key value map.
func (vm *VarMap) LoadMap() map[string]string {
	vm.mu.Lock()
	defer vm.mu.Unlock()

	m := make(map[string]string, len(vm.m))
	for k, v := range vm.m {
		m[k] = v
	}
	return m
}

// Delta returns the changes to VarMap since the last call of Delta.
func (vm *VarMap) Delta() (upd map[string]string, del map[string]bool) {
	if !vm.HasUpdates() {
		return nil, nil
	}
	vm.mu.Lock()
	defer vm.mu.Unlock()

	upd = make(map[string]string)
	del = make(map[string]bool)

	for k, v := range vm.c {
		if v { // delete
			del[k] = true
		} else { // update
			upd[k] = vm.m[k]
		}
	}
	vm.c = make(map[string]bool)
	atomic.StoreInt32(&vm.updated, 0)
	return upd, del
}
