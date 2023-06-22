// SPDX-FileCopyrightText: 2023 Richard Hansen <rhansen@rhansen.org> and contributors
// SPDX-License-Identifier: Apache-2.0

package batcher_test

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/jonboulle/clockwork"
	"github.com/rhansen/go-batcher/batcher"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

func TestBatchProcessorSliceSlice(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b := batcher.New[int, string](ctx, func(ctx context.Context, qs []int) ([]string, []error) {
		rs := make([]string, 0, len(qs))
		errs := make([]error, 0, len(qs))
		for _, q := range qs {
			rs = append(rs, fmt.Sprintf("%v", q))
			errs = append(errs, fmt.Errorf("injected %v", q))
		}
		return rs, errs
	})
	const N = 10
	rs := make([]string, N)
	errs := make([]error, N)
	var gr sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		gr.Add(1)
		go func() {
			defer gr.Done()
			rs[i], errs[i] = b.Add(ctx, i)
		}()
	}
	gr.Wait()
	for i := 0; i < N; i++ {
		assert.Equal(t, fmt.Sprintf("%v", i), rs[i])
		assert.EqualError(t, errs[i], fmt.Sprintf("injected %v", i))
	}
}

func TestBatchProcessorSliceSingle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b := batcher.New[int, string](ctx, func(ctx context.Context, qs []int) ([]string, error) {
		rs := make([]string, 0, len(qs))
		for _, q := range qs {
			rs = append(rs, fmt.Sprintf("%v", q))
		}
		return rs, fmt.Errorf("injected")
	})
	const N = 10
	rs := make([]string, N)
	errs := make([]error, N)
	var gr sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		gr.Add(1)
		go func() {
			defer gr.Done()
			rs[i], errs[i] = b.Add(ctx, i)
		}()
	}
	gr.Wait()
	for i := 0; i < N; i++ {
		assert.Equal(t, fmt.Sprintf("%v", i), rs[i])
		assert.EqualError(t, errs[i], "injected")
	}
}

func TestBatchProcessorSingleSingle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	b := batcher.New[int, string](ctx, func(ctx context.Context, qs []int) (string, error) {
		return "response", fmt.Errorf("injected")
	})
	const N = 10
	rs := make([]string, N)
	errs := make([]error, N)
	var gr sync.WaitGroup
	for i := 0; i < N; i++ {
		i := i
		gr.Add(1)
		go func() {
			defer gr.Done()
			rs[i], errs[i] = b.Add(ctx, i)
		}()
	}
	gr.Wait()
	for i := 0; i < N; i++ {
		assert.Equal(t, "response", rs[i])
		assert.EqualError(t, errs[i], "injected")
	}
}

// quiesce avoids race conditions by giving goroutines time to reach their select statements.
// Unfortunately, I believe sleep is the only option here -- I don't think there is a way to
// determine whether another goroutine is blocked on send/receive for a particular channel without
// actually sending/receiving.
func quiesce() <-chan time.Time { return time.After(10 * time.Millisecond) }

type processRes struct {
	rs   []string
	errs []error
}

type processReq struct {
	qs    []int
	resCh chan<- processRes
}

type addCall struct {
	cancel      func(error)
	done        <-chan struct{}
	r           string
	err         error
	cancelCause error
}

type batcherTest struct {
	tCtx        context.Context
	t           *testing.T
	cancel      func(error)
	b           *batcher.Batcher[int, string]
	pending     map[int]*addCall
	addFromCh   map[string]chan int
	addFromDone map[string]<-chan struct{}
	processCh   chan processReq
	inFlightN   atomic.Int32
	inFlight    map[string]processReq
	clk         clockwork.FakeClock
}

func newBatcherTest(t *testing.T, opts ...batcher.Option) *batcherTest {
	t.Parallel()
	t.Helper()
	tCtx, tCtxCancel := context.WithCancel(context.Background())
	t.Cleanup(tCtxCancel)
	ctx, cancel := context.WithCancelCause(tCtx)
	tc := &batcherTest{
		tCtx:        tCtx,
		t:           t,
		cancel:      cancel,
		pending:     map[int]*addCall{},
		addFromCh:   map[string]chan int{},
		addFromDone: map[string]<-chan struct{}{},
		processCh:   make(chan processReq),
		inFlight:    map[string]processReq{},
		clk:         clockwork.NewFakeClock(),
	}
	opts = append([]batcher.Option{batcher.WithClock(tc.clk)}, opts...)
	tc.b = batcher.New[int, string](ctx, func(ctx context.Context, qs []int) ([]string, []error) {
		tc.inFlightN.Add(1)
		defer tc.inFlightN.Add(-1)
		resCh := make(chan processRes)
		tc.processCh <- processReq{qs: qs, resCh: resCh}
		res := <-resCh
		if ctx.Err() != nil {
			err := fmt.Errorf("Batcher is shutting down: %w", context.Cause(ctx))
			rs := make([]string, 0, len(qs))
			errs := make([]error, 0, len(qs))
			for range qs {
				rs = append(rs, "")
				errs = append(errs, err)
			}
			return rs, errs
		}
		return res.rs, res.errs
	}, opts...)
	return tc
}

func (tc *batcherTest) StartAdd(q int) *batcherTest {
	tc.t.Helper()
	// This does not use the run context because we want to be able to test canceling the run context
	// without canceling the add context and vice-versa.
	ctx, cancel := context.WithCancelCause(tc.tCtx)
	done := make(chan struct{})
	qr := &addCall{
		cancel: cancel,
		done:   done,
	}
	tc.pending[q] = qr
	go func() {
		defer close(done)
		qr.r, qr.err = tc.b.Add(ctx, q)
		if ctx.Err() != nil {
			qr.cancelCause = context.Cause(ctx)
		}
	}()
	// Give the Batcher's select loop some time to pick up the new value.
	<-quiesce()
	return tc
}

func (tc *batcherTest) CheckAddBlocked(q int) *batcherTest {
	tc.t.Helper()
	select {
	case <-tc.pending[q].done:
		tc.t.Fatalf("Add has already returned")
	case <-quiesce():
	}
	return tc
}

func (tc *batcherTest) CancelAdd(q int, cause error) *batcherTest {
	tc.t.Helper()
	tc.pending[q].cancel(cause)
	return tc
}

func (tc *batcherTest) CheckAddReturned(q int, wantR, wantErr, wantCancelCause string) *batcherTest {
	tc.t.Helper()
	qr := tc.pending[q]
	select {
	case <-qr.done:
	case <-quiesce():
		tc.t.Fatalf("add has not returned")
	}
	delete(tc.pending, q)
	assert.Equal(tc.t, wantR, qr.r)
	if wantErr == "" {
		assert.NoError(tc.t, qr.err)
	} else {
		assert.EqualError(tc.t, qr.err, wantErr)
	}
	if wantCancelCause == "" {
		assert.NoError(tc.t, qr.cancelCause)
	} else {
		assert.EqualError(tc.t, qr.cancelCause, wantCancelCause)
	}
	return tc
}

func (tc *batcherTest) InputChAdd(name string) *batcherTest {
	tc.t.Helper()
	tc.addFromCh[name] = make(chan int)
	tc.addFromDone[name] = tc.b.AddFrom(tc.addFromCh[name])
	return tc
}

func (tc *batcherTest) InputChSend(name string, q int) *batcherTest {
	tc.t.Helper()
	select {
	case tc.addFromCh[name] <- q:
	case <-quiesce():
		tc.t.Errorf("input channel %v not ready to receive", name)
	}
	return tc
}

func (tc *batcherTest) InputChClose(name string) *batcherTest {
	tc.t.Helper()
	close(tc.addFromCh[name])
	delete(tc.addFromCh, name)
	return tc
}

func (tc *batcherTest) CheckInputChDone(name string, want bool) *batcherTest {
	tc.t.Helper()
	got := false
	select {
	case <-tc.addFromDone[name]:
		got = true
		delete(tc.addFromDone, name)
	case <-quiesce():
	}
	assert.Equal(tc.t, want, got)
	return tc
}

func (tc *batcherTest) StartProcessing(id string, want []int) *batcherTest {
	tc.t.Helper()
	select {
	case pq := <-tc.processCh:
		tc.inFlight[id] = pq
		assert.Equal(tc.t, want, pq.qs)
	case <-quiesce():
		tc.t.Fatalf("batch processor has not been called")
	}
	return tc
}

func (tc *batcherTest) FinishProcessing(id string) *batcherTest {
	tc.t.Helper()
	pq := tc.inFlight[id]
	pr := processRes{}
	for _, q := range pq.qs {
		pr.rs = append(pr.rs, fmt.Sprintf("%v", q))
		pr.errs = append(pr.errs, fmt.Errorf("injected %v", q))
	}
	select {
	case pq.resCh <- pr:
	case <-quiesce():
		tc.t.Fatalf("batch processor not ready to receive results")
	}
	delete(tc.inFlight, id)
	return tc
}

func (tc *batcherTest) Process(want []int) *batcherTest {
	tc.t.Helper()
	return tc.StartProcessing("tmp", want).FinishProcessing("tmp")
}

func (tc *batcherTest) CheckInFlight(want int) *batcherTest {
	tc.t.Helper()
	got := int(tc.inFlightN.Load())
	assert.Equal(tc.t, want, got)
	return tc
}

func (tc *batcherTest) CancelRunCtx(err error) *batcherTest {
	tc.t.Helper()
	tc.cancel(err)
	return tc
}

func (tc *batcherTest) CheckDone(want bool) *batcherTest {
	tc.t.Helper()
	got := false
	select {
	case <-tc.b.Done():
		got = true
	case <-quiesce():
	}
	assert.Equal(tc.t, want, got)
	return tc
}

func (tc *batcherTest) AdvanceClock(d time.Duration) *batcherTest {
	tc.t.Helper()
	tc.clk.Advance(d)
	return tc
}

func (tc *batcherTest) TODO(msg string) *batcherTest {
	tc.t.Helper()
	tc.t.Errorf(fmt.Sprintf("TODO: %s", msg))
	return tc
}

func TestBatcherEmpty(t *testing.T) {
	newBatcherTest(t).
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestBatcherSingle(t *testing.T) {
	newBatcherTest(t).
		StartAdd(0).
		StartProcessing("b0", []int{0}).
		CheckAddBlocked(0).
		FinishProcessing("b0").
		CheckAddReturned(0, "0", "injected 0", "").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestBatcherAddAfterDone(t *testing.T) {
	newBatcherTest(t).
		CancelRunCtx(fmt.Errorf("canceled runCtx")).
		CheckDone(true).
		StartAdd(0).
		// There is no cancel cause because that is for the add context, which has not yet been canceled
		// (the run context and the add context are siblings, not parent-child).
		CheckAddReturned(0, "", "context canceled", "")
}

func TestBatcherCancelCancelsProcessor(t *testing.T) {
	newBatcherTest(t).
		StartAdd(0).
		StartProcessing("b0", []int{0}).
		CancelRunCtx(fmt.Errorf("canceled runCtx")).
		CheckAddBlocked(0).
		FinishProcessing("b0").
		CheckAddReturned(0, "", "Batcher is shutting down: canceled runCtx", "").
		CheckDone(true)
}

func TestBatcherCancelFlushes(t *testing.T) {
	newBatcherTest(t).
		StartAdd(0).
		StartProcessing("first", []int{0}).
		// Add a second value while the first batch is still in flight.
		StartAdd(1).
		CancelRunCtx(fmt.Errorf("canceled runCtx")).
		CheckAddBlocked(0).
		FinishProcessing("first").
		CheckAddReturned(0, "", "Batcher is shutting down: canceled runCtx", "").
		StartProcessing("second", []int{1}).
		CheckAddBlocked(1).
		CheckDone(false).
		FinishProcessing("second").
		CheckAddReturned(1, "", "Batcher is shutting down: canceled runCtx", "").
		CheckDone(true)
}

func TestBatcherCancelAdd(t *testing.T) {
	// It would be nice to also test cancelling the Add context before calling Add, but the
	// pseudorandom select case selection means there's a 50%/50% chance the value will be picked up
	// by the select loop so the test would be unreliable.
	newBatcherTest(t).
		StartAdd(0).
		StartProcessing("b0", []int{0}).
		CheckAddBlocked(0).
		CancelAdd(0, fmt.Errorf("canceled add context")).
		CheckAddReturned(0, "", "context canceled", "canceled add context").
		FinishProcessing("b0").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestDeferMaxInFlightSoftDefault(t *testing.T) {
	newBatcherTest(t, batcher.WithDeferMaxInFlightHard(2)).
		StartAdd(0).
		StartProcessing("b0", []int{0}).
		StartAdd(1).
		CheckInFlight(1).
		FinishProcessing("b0").
		CheckAddReturned(0, "0", "injected 0", "").
		Process([]int{1}).
		CheckAddReturned(1, "1", "injected 1", "").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestDeferMaxInFlightSoft(t *testing.T) {
	newBatcherTest(t,
		batcher.WithDeferMaxInFlightHard(3),
		batcher.WithDeferMaxInFlightSoft(2)).
		StartAdd(0).
		StartProcessing("b0", []int{0}).
		StartAdd(1).
		StartProcessing("b1", []int{1}).
		FinishProcessing("b0").
		CheckAddReturned(0, "0", "injected 0", "").
		FinishProcessing("b1").
		CheckAddReturned(1, "1", "injected 1", "").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestDeferMaxInFlightHardDefault(t *testing.T) {
	newBatcherTest(t,
		batcher.WithDeferMaxInFlightSoft(1),
		batcher.WithConstrainMaxAge(time.Second)).
		StartAdd(0).
		StartProcessing("b0", []int{0}).
		StartAdd(1).
		AdvanceClock(2*time.Second).
		CheckInFlight(1).
		FinishProcessing("b0").
		CheckAddReturned(0, "0", "injected 0", "").
		Process([]int{1}).
		CheckAddReturned(1, "1", "injected 1", "").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestDeferMaxInFlightHard(t *testing.T) {
	newBatcherTest(t,
		batcher.WithDeferMaxInFlightHard(2),
		batcher.WithDeferMaxInFlightSoft(1),
		batcher.WithConstrainMaxAge(time.Second)).
		StartAdd(0).
		StartProcessing("b0", []int{0}).
		StartAdd(1).
		CheckInFlight(1).
		AdvanceClock(2*time.Second).
		StartProcessing("b1", []int{1}).
		StartAdd(2).
		CheckInFlight(2).
		AdvanceClock(2*time.Second).
		CheckInFlight(2).
		FinishProcessing("b0").
		CheckAddReturned(0, "0", "injected 0", "").
		StartProcessing("b2", []int{2}).
		FinishProcessing("b1").
		CheckAddReturned(1, "1", "injected 1", "").
		FinishProcessing("b2").
		CheckAddReturned(2, "2", "injected 2", "").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestDeferMinCountSoftDefault(t *testing.T) {
	newBatcherTest(t).
		CheckInFlight(0). // Defer until 1 value has been added -- no empty batch should be sent.
		StartAdd(0).
		Process([]int{0}).
		CheckAddReturned(0, "0", "injected 0", "").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestDeferMinCountSoft(t *testing.T) {
	newBatcherTest(t, batcher.WithDeferMinCountSoft(2)).
		StartAdd(0).
		CheckInFlight(0). // Processing doesn't start until the second value is added.
		StartAdd(1).
		Process([]int{0, 1}).
		CheckAddReturned(0, "0", "injected 0", "").
		CheckAddReturned(1, "1", "injected 1", "").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestDeferMinCountHardDefault(t *testing.T) {
	newBatcherTest(t,
		batcher.WithDeferMinCountSoft(2),
		batcher.WithConstrainMaxAge(time.Second)).
		StartAdd(0).
		CheckInFlight(0).
		AdvanceClock(2*time.Second).
		Process([]int{0}).
		CheckAddReturned(0, "0", "injected 0", "").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestDeferMinCountHard(t *testing.T) {
	newBatcherTest(t,
		batcher.WithDeferMinCountHard(2),
		batcher.WithDeferMinCountSoft(3),
		batcher.WithConstrainMaxAge(time.Second)).
		StartAdd(0).
		CheckInFlight(0).
		StartAdd(1).
		CheckInFlight(0).
		AdvanceClock(2*time.Second).
		Process([]int{0, 1}).
		CheckAddReturned(0, "0", "injected 0", "").
		CheckAddReturned(1, "1", "injected 1", "").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestDeferMinCountHardIgnoredAtShutdown(t *testing.T) {
	newBatcherTest(t, batcher.WithDeferMinCountHard(2)).
		StartAdd(0).
		CheckInFlight(0).
		CancelRunCtx(nil).
		Process([]int{0}).
		CheckAddReturned(0, "", "Batcher is shutting down: context canceled", "").
		CheckDone(true)
}

func TestDeferMinAgeSoftDefault(t *testing.T) {
	newBatcherTest(t).
		StartAdd(0).
		Process([]int{0}).
		CheckAddReturned(0, "0", "injected 0", "").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestDeferMinAgeSoft(t *testing.T) {
	newBatcherTest(t, batcher.WithDeferMinAgeSoft(time.Second)).
		StartAdd(0).
		CheckInFlight(0).
		AdvanceClock(999*time.Millisecond).
		CheckInFlight(0).
		AdvanceClock(time.Millisecond).
		Process([]int{0}).
		CheckAddReturned(0, "0", "injected 0", "").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestDeferMinAgeHardDefault(t *testing.T) {
	newBatcherTest(t,
		batcher.WithDeferMinAgeSoft(time.Second),
		batcher.WithConstrainMaxCount(2)).
		StartAdd(0).
		CheckInFlight(0).
		StartAdd(1).
		Process([]int{0, 1}).
		CheckAddReturned(0, "0", "injected 0", "").
		CheckAddReturned(1, "1", "injected 1", "").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestDeferMinAgeHard(t *testing.T) {
	newBatcherTest(t,
		batcher.WithDeferMinAgeHard(time.Second),
		batcher.WithDeferMinAgeSoft(2*time.Second),
		batcher.WithConstrainMaxCount(2)).
		StartAdd(0).
		CheckInFlight(0).
		StartAdd(1).
		CheckInFlight(0).
		AdvanceClock(999*time.Millisecond).
		CheckInFlight(0).
		AdvanceClock(time.Millisecond).
		Process([]int{0, 1}).
		CheckAddReturned(0, "0", "injected 0", "").
		CheckAddReturned(1, "1", "injected 1", "").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestDeferRateLimitSoft(t *testing.T) {
	newBatcherTest(t,
		batcher.WithDeferRateLimitSoft(rate.NewLimiter(1.0, 1)),
		batcher.WithConstrainMaxCount(2)).
		StartAdd(0).
		Process([]int{0}).
		CheckAddReturned(0, "0", "injected 0", "").
		StartAdd(1).
		CheckInFlight(0).
		AdvanceClock(999*time.Millisecond).
		CheckInFlight(0).
		AdvanceClock(time.Millisecond).
		Process([]int{1}).
		CheckAddReturned(1, "1", "injected 1", "").
		StartAdd(2).
		StartAdd(3).
		Process([]int{2, 3}).
		CheckAddReturned(2, "2", "injected 2", "").
		CheckAddReturned(3, "3", "injected 3", "").
		StartAdd(4).
		// The token added at t=1 (now) was consumed by b1. The token that will be added at t=2 will be
		// consumed by b2 (the number of tokens in the bucket is effectively negative right now).
		// Finally at t=3, two seconds from now, a token will be available for the batch containing q=4.
		AdvanceClock(1999*time.Millisecond).
		CheckInFlight(0).
		AdvanceClock(time.Millisecond).
		Process([]int{4}).
		CheckAddReturned(4, "4", "injected 4", "").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestDeferRateLimitHard(t *testing.T) {
	newBatcherTest(t,
		batcher.WithDeferRateLimitHard(rate.NewLimiter(2.0, 1)),
		batcher.WithDeferRateLimitSoft(rate.NewLimiter(1.0, 1)),
		batcher.WithConstrainMaxCount(2)).
		StartAdd(0).
		Process([]int{0}).
		CheckAddReturned(0, "0", "injected 0", "").
		StartAdd(1).
		StartAdd(2).
		AdvanceClock(499*time.Millisecond).
		CheckInFlight(0).
		AdvanceClock(time.Millisecond).
		Process([]int{1, 2}).
		CheckAddReturned(1, "1", "injected 1", "").
		CheckAddReturned(2, "2", "injected 2", "").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestConstrainMaxAgeDefault(t *testing.T) {
	newBatcherTest(t, batcher.WithDeferMinCountSoft(2)).
		StartAdd(0).
		CheckInFlight(0).
		AdvanceClock((math.MaxInt64-1)*time.Nanosecond).
		CheckInFlight(0).
		CancelRunCtx(nil).
		Process([]int{0}).
		CheckAddReturned(0, "", "Batcher is shutting down: context canceled", "").
		CheckDone(true)
}

func TestConstrainMaxAge(t *testing.T) {
	newBatcherTest(t,
		batcher.WithDeferMinCountSoft(2),
		batcher.WithConstrainMaxAge(time.Second)).
		StartAdd(0).
		CheckInFlight(0).
		AdvanceClock(999*time.Millisecond).
		CheckInFlight(0).
		AdvanceClock(1*time.Millisecond).
		Process([]int{0}).
		CheckAddReturned(0, "0", "injected 0", "").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestConstrainMaxCount(t *testing.T) {
	newBatcherTest(t,
		batcher.WithDeferMinAgeSoft(time.Second),
		batcher.WithConstrainMaxCount(2)).
		StartAdd(0).
		CheckInFlight(0).
		StartAdd(1).
		Process([]int{0, 1}).
		CheckAddReturned(0, "0", "injected 0", "").
		CheckAddReturned(1, "1", "injected 1", "").
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestAddFromDone(t *testing.T) {
	newBatcherTest(t).
		InputChAdd("in0").
		InputChSend("in0", 0).
		Process([]int{0}).
		InputChClose("in0").
		CheckInputChDone("in0", true).
		CheckDone(false).
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestAddFromMultiple(t *testing.T) {
	newBatcherTest(t).
		InputChAdd("in0").
		InputChAdd("in1").
		InputChSend("in0", 0).
		Process([]int{0}).
		InputChSend("in1", 1).
		Process([]int{1}).
		InputChClose("in0").
		CheckInputChDone("in0", true).
		CheckInputChDone("in1", false).
		InputChClose("in1").
		CheckInputChDone("in1", true).
		CheckDone(false).
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestAddFromNotClosed(t *testing.T) {
	newBatcherTest(t).
		InputChAdd("in0").
		CancelRunCtx(nil).
		CheckInputChDone("in0", true).
		CheckDone(true)
}

func TestAddFromMixedWithAdd(t *testing.T) {
	newBatcherTest(t, batcher.WithDeferMinCountSoft(3)).
		InputChAdd("in0").
		InputChSend("in0", 0).
		StartAdd(1).
		InputChSend("in0", 2).
		Process([]int{0, 1, 2}).
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestAddFromSecondAfterFirst(t *testing.T) {
	newBatcherTest(t).
		InputChAdd("in0").
		InputChClose("in0").
		CheckInputChDone("in0", true).
		InputChAdd("in1").
		InputChSend("in1", 0).
		Process([]int{0}).
		CancelRunCtx(nil).
		CheckDone(true)
}

func TestToChan(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ch := make(chan []string)
	b := batcher.ToChan(ctx, ch)
	addReturned := make(chan struct{})
	go func() {
		b.Add(ctx, "foo")
		close(addReturned)
	}()
	select {
	case <-addReturned:
		t.Errorf("Add returned before batch was sent")
	case got, ok := <-ch:
		if !ok {
			t.Errorf("output channel closed unexpectedly")
		} else if diff := cmp.Diff([]string{"foo"}, got); diff != "" {
			t.Errorf("received batch differs from expected; diff from -want to +got:\n%s", diff)
		}
	case <-time.After(time.Second):
		t.Errorf("timed out waiting for batch")
	}
}

func TestToChanDoesNotCloseOutpuTChan(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ch := make(chan []string)
	b := batcher.ToChan(ctx, ch)
	cancel()
	<-b.Done()
	select {
	case _, ok := <-ch:
		if ok {
			t.Errorf("unexpected batch")
		} else {
			t.Errorf("output channel closed unexpectedly")
		}
	default:
	}
}

func TestChanBackloggedReceiver(t *testing.T) {
	inCh := make(chan string)
	outCh := batcher.Chan(inCh)
	want := []string{"foo", "bar", "baz"}
	for _, v := range want {
		select {
		case inCh <- v:
		case <-time.After(time.Second):
			t.Fatalf("input value send timed out")
		}
	}
	close(inCh)
	select {
	case got, ok := <-outCh:
		if !ok {
			t.Fatalf("output channel closed unexpectedly")
		} else if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("batch differs from expected; diff from -want to +got:\n%s", diff)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for batch")
	}
	select {
	case _, ok := <-outCh:
		if ok {
			t.Errorf("received unexpected batch; want channel to be closed")
		}
	case <-time.After(time.Second):
		t.Errorf("timed out waiting for output channel to be closed")
	}
}
