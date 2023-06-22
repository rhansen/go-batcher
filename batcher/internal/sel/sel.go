// SPDX-FileCopyrightText: 2023 Richard Hansen <rhansen@rhansen.org> and contributors
// SPDX-License-Identifier: Apache-2.0

// Package sel makes it easier to work with dynamic select cases.
package sel

import (
	"fmt"
	"reflect"
)

type CaseId int

type SelectStatement struct {
	cases []reflect.SelectCase
	// ids maps an index of the cases slice to the public case ID corresponding to that case.
	ids []CaseId
	// indexes maps a case ID to an index into cases, or -1 if no such case exists yet.
	indexes []int
}

func (ss *SelectStatement) Select() (CaseId, any, bool) {
	i, vv, ok := reflect.Select(ss.cases)
	var v any
	if vv.IsValid() {
		v = vv.Interface()
	}
	return ss.ids[i], v, ok
}

func (ss *SelectStatement) SetChan(id CaseId, ch any, send bool) {
	for i := len(ss.indexes); i <= int(id); i++ {
		ss.indexes = append(ss.indexes, -1)
	}
	i := ss.indexes[id]
	chv := reflect.ValueOf(ch)
	if chv.IsValid() && chv.Kind() != reflect.Chan {
		panic(fmt.Errorf("not a channel: %v", ch))
	}
	if !chv.IsValid() || chv.IsZero() {
		if i >= 0 {
			// Delete an existing case. Alternatively the .Chan field could be set to nil, which would
			// cause the case to be ignored (sending/receiving on nil blocks forever). However, the select
			// cases are expected to be used in a tight loop (where the Select call frequency is much
			// greater than the frequency of nil/non-nil toggles) so it is worthwhile to remove the case
			// now to avoid repeated wasted effort. This also prevents cruft accumulation.
			ss.indexes[id] = -1
			copy(ss.cases[i:len(ss.cases)-1], ss.cases[i+1:])
			ss.cases = ss.cases[:len(ss.cases)-1]
			copy(ss.ids[i:len(ss.ids)-1], ss.ids[i+1:])
			ss.ids = ss.ids[:len(ss.ids)-1]
			for newi := i; newi < len(ss.ids); newi++ {
				ss.indexes[ss.ids[newi]] = newi
			}
		}
	} else {
		dir := reflect.SelectRecv
		if send {
			dir = reflect.SelectSend
		}
		if i < 0 {
			// Insert a new case.
			ss.cases = append(ss.cases, reflect.SelectCase{Dir: dir, Chan: chv})
			ss.ids = append(ss.ids, id)
			ss.indexes[id] = len(ss.ids) - 1
		} else {
			// Update an existing case.
			c := &ss.cases[i]
			c.Dir = dir
			c.Chan = chv
			if !send {
				c.Send = reflect.Value{}
			}
		}
	}
}

func (ss *SelectStatement) SetSend(id CaseId, v any) {
	ss.cases[ss.indexes[id]].Send = reflect.ValueOf(v)
}
