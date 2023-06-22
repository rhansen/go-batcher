// SPDX-FileCopyrightText: 2023 Richard Hansen <rhansen@rhansen.org> and contributors
// SPDX-License-Identifier: Apache-2.0

package sel

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestSetChan(t *testing.T) {
	chs := make([]chan struct{}, 4)
	for i := range chs {
		chs[i] = make(chan struct{})
	}
	var nilCh chan struct{}
	ss := &SelectStatement{}

	for i, tc := range []struct {
		id            CaseId
		ch            chan struct{}
		wantCaseChans []any
		wantIds       []CaseId
		wantIndexes   []int
	}{
		{2, nil, nil, nil, []int{-1, -1, -1}},
		{1, nil, nil, nil, []int{-1, -1, -1}},
		{1, chs[1], []any{chs[1]}, []CaseId{1}, []int{-1, 0, -1}},
		// Update an existing case with the same channel.
		{1, chs[1], []any{chs[1]}, []CaseId{1}, []int{-1, 0, -1}},
		// Update an existing case with a new channel.
		{1, chs[2], []any{chs[2]}, []CaseId{1}, []int{-1, 0, -1}},
		{1, nil, nil, nil, []int{-1, -1, -1}},
		{3, chs[3], []any{chs[3]}, []CaseId{3}, []int{-1, -1, -1, 0}},
		{3, nilCh, nil, nil, []int{-1, -1, -1, -1}},
		// Populate all cases (in reverse order to demonstrate append effect).
		{3, chs[3], []any{chs[3]}, []CaseId{3}, []int{-1, -1, -1, 0}},
		{2, chs[2], []any{chs[3], chs[2]}, []CaseId{3, 2}, []int{-1, -1, 1, 0}},
		{1, chs[1], []any{chs[3], chs[2], chs[1]}, []CaseId{3, 2, 1}, []int{-1, 2, 1, 0}},
		{0, chs[0], []any{chs[3], chs[2], chs[1], chs[0]}, []CaseId{3, 2, 1, 0}, []int{3, 2, 1, 0}},
		// Clear case ID 0 (currently the last case).
		{0, nil, []any{chs[3], chs[2], chs[1]}, []CaseId{3, 2, 1}, []int{-1, 2, 1, 0}},
		// Restore case ID 0.
		{0, chs[0], []any{chs[3], chs[2], chs[1], chs[0]}, []CaseId{3, 2, 1, 0}, []int{3, 2, 1, 0}},
		// Clear case ID 3 (currently the first case).
		{3, nil, []any{chs[2], chs[1], chs[0]}, []CaseId{2, 1, 0}, []int{2, 1, 0, -1}},
		// Restoring case ID 3 will put it at the end.
		{3, chs[3], []any{chs[2], chs[1], chs[0], chs[3]}, []CaseId{2, 1, 0, 3}, []int{2, 1, 0, 3}},
	} {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			ss.SetChan(tc.id, tc.ch, false)
			gotCaseChans := make([]any, 0, len(ss.cases))
			for i, c := range ss.cases {
				gotCaseChans = append(gotCaseChans, c.Chan.Interface())
				if got, want := c.Dir, reflect.SelectRecv; got != want {
					t.Errorf("case %v has wrong direction; got %v, want %v", i, got, want)
				}
			}
			if diff := cmp.Diff(tc.wantCaseChans, gotCaseChans, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("case channels differ; diff from -want to +got:\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantIds, ss.ids, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ids differ; diff from -want to +got:\n%s", diff)
			}
			if diff := cmp.Diff(tc.wantIndexes, ss.indexes, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("indexes differ; diff from -want to +got:\n%s", diff)
			}
		})
	}
}

func TestSetChanChangeDirection(t *testing.T) {
	ch := make(chan int)
	ss := &SelectStatement{}
	for i, tc := range []struct {
		send  bool
		sendv reflect.Value
		wantv reflect.Value
	}{
		{send: false},
		{send: true},
		{send: true, sendv: reflect.ValueOf(42), wantv: reflect.ValueOf(42)},
		// Switching back to receive should clear the send value.
		{send: false},
		// Switching back to send should not restore the send value.
		{send: true},
	} {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			ss.SetChan(0, ch, tc.send)
			if tc.send && tc.sendv.IsValid() {
				ss.SetSend(0, tc.sendv.Interface())
			}
			wantDir := reflect.SelectRecv
			if tc.send {
				wantDir = reflect.SelectSend
			}
			if got := ss.cases[0].Dir; got != wantDir {
				t.Errorf("got case direction %v, want %v", got, wantDir)
			}
			if got, want := ss.cases[0].Chan.Interface(), any(ch); got != want {
				t.Errorf("got case channel %v, want %v", got, want)
			}
			if got, want := ss.cases[0].Send, tc.wantv; got != want {
				t.Errorf("got case send value %v, want %v", got, want)
			}
		})
	}
}

func TestSelect(t *testing.T) {
	ss := &SelectStatement{}
	ch0 := make(chan int)
	ch1 := make(chan string)
	ch2 := make(chan string)
	// Reverse order to test case->ID mapping.
	ss.SetChan(2, ch2, false)
	ss.SetChan(1, ch1, false)
	ss.SetChan(0, ch0, true)
	ss.SetSend(0, 42)
	for i, tc := range []struct {
		action func()
		wantId CaseId
		wantV  any
		wantOk bool
	}{
		{func() { <-ch0 }, 0, nil, false},
		{func() { ch1 <- "foo" }, 1, "foo", true},
		{func() { close(ch2) }, 2, "", false},
	} {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			go tc.action()
			gotId, gotV, gotOk := ss.Select()
			if gotId != tc.wantId {
				t.Errorf("got ID %v, want %v", gotId, tc.wantId)
			}
			if gotV != tc.wantV {
				t.Errorf("got receive value %v, want %v", gotV, tc.wantV)
			}
			if gotOk != tc.wantOk {
				t.Errorf("got receive ok %v, want %v", gotOk, tc.wantOk)
			}
		})
	}
}
