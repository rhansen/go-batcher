// SPDX-FileCopyrightText: 2023 Richard Hansen <rhansen@rhansen.org> and contributors
// SPDX-License-Identifier: Apache-2.0

// Package batcher collects values and dispatches them in batches for amortizing processing cost or
// limiting processing concurrency.
//
// # Inputs and Outputs
//
// A [Batcher] can acquire new values in the following ways, which may be combined:
//   - Call the [Batcher.Add] method.
//   - Send values to a channel registered with the [Batcher.AddFrom] method.
//
// A Batcher can output batches in one of the following ways:
//   - Call a user-supplied [BatchProcessor] function.
//   - Send the batch to an output channel passed to [ToChan].
//
// # Configurable Behaviors
//
// By default, a new non-empty batch is dispatched immediately after a new value is received.
// Exception: If the previous batch is still being processed, a new batch will be dispatched once
// the previous batch has been processed. Thus, each batch typically has only one value, unless
// batch processing is slow relative to the frequency of new values.
//
// Options are availble to change the behavior to meet your requirements.
//
// ## Defer Thresholds
//
// Defer thresholds are set via [Option]s returned from the WithDefer* family of functions. A new
// batch is not dispatched until all defer thresholds are met, allowing more values to accumulate in
// the new batch. Thus, changing a defer threshold can change how often accumulated values are
// dispatched as a batch, as well as the size of those batches.
//
// Each type of defer threshold has two subtypes: soft and hard. A soft threshold is a wish-to-have
// threshold that will be violated if necessary to satisfy a dispatch constraint (discussed below).
// A hard threshold is a must-have threshold that will not be violated even if it means violating a
// dispatch constraint, with one exception: when the stream of input values is finished, any hard
// thresholds that cannot possibly be met by pending values (values received but not yet dispatched)
// are ignored when deciding when to dispatch the final batch.
//
// The following defer thresholds are currently configurable:
//
//   - MaxInFlight: The maximum number of batches that can be "in flight". (The number of in-flight
//     batches is the number of calls to the [BatchProcessor] that have not yet returned.) Defaults
//     to 1 (hard and soft).
//   - MinCount: The minimum number of values in a batch. Defaults to 1 (hard and soft).
//   - MinAge: The minimum age of the youngest value in the batch. Defaults to 0s (hard and soft).
//   - RateLimit: Defer dispatching the next batch if the number of recently dispatched batches
//     exceeds a limit. Defaults to infinite (hard and soft).
//
// TODO: The following defer thresholds are not yet implemented and thus not configurable, but they
// should be straightforward to implement if desired:
//
//   - MinWeight: Minimum "weight" of a batch. (A batch's weight would be determined by the
//     application, "number of bytes" for example.) Currently this is effectively 0 (both soft and
//     hard), meaning batch weight is ignored.
//   - MaxInFlightCount: Maximum total number of values across all in-flight batches. Currently this
//     is effectively infinite (both soft and hard), meaning there is no limit to the number of
//     in-flight values.
//   - MaxInFlightWeight: Maximum total weight of all in-flight batches. Currently this is
//     effectively infinite (both soft and hard), meaning there is no limit to the number of
//     in-flight values.
//
// ## Dispatch Constraints
//
// Dispatch constraints are set via [Option]s returned from the WithConstrain* family of functions.
// While defer thresholds control when batches should NOT be dispatched, dispatch constraints are
// the opposite: they control when batches SHOULD be dispatched. If any dispatch constraint is
// satisfied, and all hard defer thresholds have been met, a new batch is dispatched even if there
// are some soft defer thresholds that have not yet been met.
//
// Dispatch constraints are useful for setting upper bounds on batch size and processing latency.
//
// The following dispatch constraints are currently configurable:
//
//   - MaxAge: The maximum age of the oldest value in the batch. Defaults to unlimited.
//   - MaxCount: Maximum number of values in a batch. Defaults to unlimited.
//
// TODO: The following dispatch constraints are not yet implemented and thus not configurable, but
// they should be straightforward to implement if desired:
//
//   - MinRate: Dispatch a new batch if the number of recently dispatched batches drops below a
//     limit. Currently this is effectively 0.
//   - MaxWeight: Maximum weight of a batch. (A newly received value would be placed in the next
//     batch if it would cause the current batch to exceed the weight limit.) Currently this is
//     effectively infinite, meaning batch weight is ignored.
//   - MinInFlight: Minimum number of in-flight batches. (A new batch is dispatched if the number of
//     in-flight batches drops below this threshold.) Currently this is effectively 0, meaning the
//     number of in-flight batches is ignored.
//   - MinInFlightCount: Minimum total number of in-flight values across all in-flight batches.
//     Currently this is effectively 0, meaning the number of in-flight values is ignored.
//   - MinInFlightWeight: Minimum total weight of all in-flight batches. Currently this is
//     effectively 0, meaning the weight is ignored.
package batcher

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/rhansen/go-batcher/batcher/internal/sel"
	"golang.org/x/time/rate"
)

// A timer is a clockwork.Timer whose zero value is a timer that can never fire (Chan returns nil,
// and Reset is a no-op). Furthermore, Stop and Reset drain the channel. Because of this, all
// channel reads and all calls to Stop or Reset must happen in the same goroutine to avoid race
// conditions.
type timer struct {
	clockwork.Timer
}

func newTimer(clk clockwork.Clock) timer {
	tmr := timer{clk.NewTimer(0)}
	<-tmr.Chan()
	return tmr
}

func (tmr timer) Chan() <-chan time.Time {
	if tmr.Timer == nil {
		return nil
	}
	return tmr.Timer.Chan()
}

func (tmr timer) Stop() bool {
	if tmr.Timer == nil {
		return false
	}
	tmr.Timer.Stop()
	select {
	case <-tmr.Chan():
	default:
	}
	return false
}

func (tmr timer) Reset(d time.Duration) bool {
	if tmr.Timer == nil {
		return false
	}
	tmr.Stop()
	tmr.Timer.Reset(d)
	return false
}

type batcherConfig struct {
	deferMaxInFlightHard int
	deferMaxInFlightSoft int
	deferMinCountHard    int
	deferMinCountSoft    int
	deferMinAgeHard      time.Duration
	deferMinAgeSoft      time.Duration
	deferRateLimitHard   *rate.Limiter
	deferRateLimitSoft   *rate.Limiter
	constrainMaxAge      time.Duration
	constrainMaxCount    int
	clock                clockwork.Clock
}

func processOptions(opts []Option) *batcherConfig {
	cfg := &batcherConfig{
		deferMaxInFlightHard: 1,
		deferMaxInFlightSoft: 1,
		deferMinCountHard:    1,
		deferMinCountSoft:    1,
		constrainMaxAge:      math.MaxInt64,
		constrainMaxCount:    math.MaxInt,
		clock:                clockwork.NewRealClock(),
	}
	for _, o := range opts {
		o(cfg)
	}
	return cfg
}

// Option customizes the batching behavior.
type Option func(*batcherConfig)

// WithDeferMaxInFlightSoft returns an [Option] that sets the soft MaxInFlight defer threshold to n,
// which must be positive. Dispatch of a new batch is postponed until the number of in-flight
// batches (the number of calls to the [BatchProcessor] function that have not yet returned) has
// dropped below this number, unless preempted by a dispatch constraint. If this option is not used,
// the soft threshold is 1.
func WithDeferMaxInFlightSoft(n int) Option {
	if n <= 0 {
		panic(fmt.Errorf("MaxInFlight defer threshold must be positive, is %v", n))
	}
	return func(cfg *batcherConfig) { cfg.deferMaxInFlightSoft = n }
}

// WithDeferMaxInFlightHard returns an [Option] that sets the hard MaxInFlight defer threshold to n,
// which must be positive. This is the same as [WithDeferMaxInFlightSoft], except it cannot be
// preempted by a dispatch constraint. If this option is not used, the hard threshold is 1.
func WithDeferMaxInFlightHard(n int) Option {
	if n <= 0 {
		panic(fmt.Errorf("MaxInFlight defer threshold must be positive, is %v", n))
	}
	return func(cfg *batcherConfig) { cfg.deferMaxInFlightHard = n }
}

// WithDeferMinCountSoft returns an [Option] that sets the soft MinCount defer threshold to n.
// Dispatch of a new batch is postponed until the number of values in the batch has reached n,
// unless preempted by a dispatch constraint. If this option is not used, the soft threshold is 1.
func WithDeferMinCountSoft(n int) Option {
	if n <= 0 {
		// TODO: Users might want empty "heartbeat" batches if the input is idle. Figure out how a
		// MinCount threshold of 0 should interact with MinAge, and how to avoid an accidental tight
		// infinite loop.
		panic(fmt.Errorf("MinCount defer threshold less than 1 is not yet supported; is %v", n))
	}
	return func(cfg *batcherConfig) { cfg.deferMinCountSoft = n }
}

// WithDeferMinCountHard returns an [Option] that sets the hard MinCount defer threshold to n. This
// is the same as [WithDeferMinCountSoft], except it cannot be preempted by a dispatch constraint.
// If this option is not used, the hard threshold is 1.
func WithDeferMinCountHard(n int) Option {
	if n <= 0 {
		panic(fmt.Errorf("MinCount defer threshold less than 1 is not yet supported; is %v", n))
	}
	return func(cfg *batcherConfig) { cfg.deferMinCountHard = n }
}

// WithDeferMinAgeSoft returns an [Option] that sets the soft MinAge defer threshold to d. Dispatch
// of a new batch is postponed until after the age of the youngest value in the batch reaches d,
// unless preempted by a dispatch constraint. If this option is not used, the soft threshold is 0.
func WithDeferMinAgeSoft(d time.Duration) Option {
	return func(cfg *batcherConfig) { cfg.deferMinAgeSoft = d }
}

// WithDeferMinAgeHard returns an [Option] that sets the hard MinAge defer threshold to d. This is
// the same as [WithDeferMinAgeSoft], except it cannot be preempted by a dispatch constraint. If
// unsure, you are encouraged to use [WithDeferMinAgeSoft] instead of this. If this option is not
// used, the hard threshold is 0.
func WithDeferMinAgeHard(d time.Duration) Option {
	return func(cfg *batcherConfig) { cfg.deferMinAgeHard = d }
}

// WithDeferRateLimitSoft returns an [Option] that sets the soft RateLimit defer threshold to d.
// Dispatch of a new batch is postponed until a token is available in lim, unless preempted by a
// dispatch constraint. If this option is not used, the soft rate is unlimited.
//
// Note: If this is preempted by a dispatch constraint, the next batch will be delayed more than
// usual to keep the overall rate under the threshold. If this is consistently preempted, this may
// become significantly backlogged resulting in a long period before the next batch is dispatched
// after the preemption ceases.
func WithDeferRateLimitSoft(lim *rate.Limiter) Option {
	return func(cfg *batcherConfig) { cfg.deferRateLimitSoft = lim }
}

// WithDeferRateLimitHard returns an [Option] that sets the hard RateLimit defer threshold to lim. This
// is the same as [WithDeferRateLimitSoft], except it cannot be preempted by a dispatch constraint. If
// unsure, you are encouraged to use [WithDeferRateLimitSoft] instead of this. If this option is not
// used, or if lim is nil, the hard rate is unlimited.
func WithDeferRateLimitHard(lim *rate.Limiter) Option {
	return func(cfg *batcherConfig) { cfg.deferRateLimitHard = lim }
}

// WithConstrainMaxAge returns an [Option] that sets the MaxAge dispatch constraint to d. A new
// batch is dispatched immediately if the age of the oldest value in the batch exceeds d and all
// hard defer thresholds have been met. If this option is not used, there is no maximum age.
func WithConstrainMaxAge(d time.Duration) Option {
	return func(cfg *batcherConfig) { cfg.constrainMaxAge = d }
}

// WithConstrainMaxCount returns an [Option] that sets the MaxCount dispatch constraint to n. A new
// batch is dispatched immediately if the number of values in the batch is n and all hard defer
// thresholds have been met. If this option is not used, there is no limit.
func WithConstrainMaxCount(n int) Option {
	return func(cfg *batcherConfig) { cfg.constrainMaxCount = n }
}

// WithClock returns an [Option] that changes the clock implementation that is used to reckon ages
// and time intervals.  Its primary purpose is to facilitate testing.  If this option is not used,
// the system's real clock is used.
func WithClock(c clockwork.Clock) Option {
	return func(cfg *batcherConfig) { cfg.clock = c }
}

// A BatchProcessor is a function that takes a batch of values (queries), processes them, and
// returns their corresponding responses and errors. A Batcher considers the batch to be "in flight"
// until this function returns. A Batcher may call this function concurrently if the number of
// in-flight batches is under the Batcher's configured MaxInFlight defer threshold.
//
// For convenience, the return values can take one of these forms:
//   - ([]R, []error): The response and error slices have length equal to the number of queries in
//     the batch, and entry i contains the response/error corresponding to query i in the batch.
//   - ([]R, error): Same as above, except a single error value applies to all queries.
//   - (R, error): The same response and error value applies to all queries.
//
// The response and error values for a query added via [Add] are propagated back to the waiting
// caller. The response and error values for a query added via an input channel registered with
// [Batcher.AddFrom] are discarded.
//
// Canceling the context passed to [New] or [ToChan] cancels the context that the Batcher passes to
// this function.
type BatchProcessor[Q, R any] interface {
	func(context.Context, []Q) ([]R, []error) |
		func(context.Context, []Q) ([]R, error) |
		func(context.Context, []Q) (R, error)
}

func normalizeBatchProcessor[Q, R any, F BatchProcessor[Q, R]](fn F) func(ctx context.Context, qs []Q, rChs []chan<- batcherR[R]) {
	if fn == nil {
		return nil
	}
	// Type switching on type parameters isn't supported yet so do a runtime type switch on the value.
	// https://go.dev/issue/45380
	switch fn := any(fn).(type) {
	case func(context.Context, []Q) ([]R, []error):
		return func(ctx context.Context, qs []Q, rChs []chan<- batcherR[R]) {
			rs, errs := fn(ctx, qs)
			for i := range qs {
				if rChs[i] == nil {
					continue
				}
				var r R
				var err error
				if i < len(rs) {
					r = rs[i]
				}
				if i < len(errs) {
					err = errs[i]
				}
				rChs[i] <- batcherR[R]{r, err}
			}
		}
	case func(context.Context, []Q) ([]R, error):
		return func(ctx context.Context, qs []Q, rChs []chan<- batcherR[R]) {
			rs, err := fn(ctx, qs)
			for i := range qs {
				if rChs[i] == nil {
					continue
				}
				var r R
				if i < len(rs) {
					r = rs[i]
				}
				rChs[i] <- batcherR[R]{r, err}
			}
		}
	case func(context.Context, []Q) (R, error):
		return func(ctx context.Context, qs []Q, rChs []chan<- batcherR[R]) {
			r, err := fn(ctx, qs)
			br := batcherR[R]{r, err}
			for i := range qs {
				if rCh := rChs[i]; rCh != nil {
					rChs[i] <- br
				}
			}
		}
	default:
		panic("assertion failed: unexpected BatchProcessor type")
	}
}

type inputChannel[Q any] struct {
	ch   <-chan Q
	done chan<- struct{}
}

// A Batcher collects values in batches, dispatching each batch to a custom [BatchProcessor] or
// channel.
type Batcher[Q, R any] struct {
	cfg *batcherConfig
	// addCh is the input channel that receives new values from Add. This channel has multiple senders
	// from multiple goroutines, so it is never closed. Instead, the select loop stops reading from it
	// once the context is canceled.
	addCh chan batcherQ[Q, R]
	// addFromCh receives channels passed to AddFrom so a case can be added to the select loop.
	// Implementation note: The values could instead be read in a separate goroutine and fed to Add,
	// but that would effectively increase buffering by one and make it look like the value will be in
	// the next batch when it might not. This matters when the channelsq are used for
	// synchronization.
	addFromCh chan inputChannel[Q]
	// inputChDones contains channels that are closed when done reading from their corresponding input
	// channels registered with AddFrom.
	inputChDones []chan<- struct{}
	// current is the current batch of queries being assembled.
	current []Q
	// rChs are the response channels used to communicate the responses to Add. Each entry corresponds
	// to a query in current.
	rChs []chan<- batcherR[R]
	// outCh is the user-supplied output channel, or nil if not provided (in which case a
	// BatchProcessor function must be provided). Implementation note: The batches could instead be
	// passed to a BatchProcessor function that relays them to the user-supplied output channel, but
	// then it would not be possible to let new queries accumulate in the batch until the user's
	// channel is ready to receive.
	outCh chan<- []Q
	// processBatch is a closure around the user-supplied BatchProcessor function, or nil if outCh is
	// to be used instead. It passes the batch to the BatchProcessor and propagates the results back
	// to the Add callers via the corresponding response channels.
	processBatch func(context.Context, []Q, []chan<- batcherR[R])
	// inFlight is a linked list of batches that are currently being processed by the user-supplied
	// BatchProcessor function.
	inFlight *list.List
	// processedCh receives each inFlight element upon completion.
	processedCh chan *list.Element
	// runCtx is canceled once input values are no longer accepted.
	runCtx context.Context
	// done is closed once all input values have been processed.
	done chan struct{}

	// defer thresholds

	deferMaxInFlightHardMet bool
	deferMaxInFlightSoftMet bool
	deferMinCountHardMet    bool
	deferMinCountSoftMet    bool
	deferMinAgeHardMet      bool
	deferMinAgeHardTimer    timer
	deferMinAgeSoftMet      bool
	deferMinAgeSoftTimer    timer
	deferRateLimitHardMet   bool
	deferRateLimitHardTimer timer
	deferRateLimitSoftMet   bool
	deferRateLimitSoftTimer timer

	// dispatch constraints

	constrainCanceledMet bool
	constrainMaxAgeMet   bool
	constrainMaxAgeTimer timer
	constrainMaxCountMet bool
}

// New returns a new [Batcher] that dispatches batches to the given [BatchProcessor] function.
//
// Canceling ctx has the following effects:
//   - The context passed to the given BatchProcessor is canceled.
//   - Any remaining values are batched and processed, after any hard defer thresholds that can be
//     met with the remaining values are met. (Hard defer thresholds such as MinCount are ignored.)
//   - No new values can be enqueued; all future calls to Add immediately return ctx's error.
//   - Once the final batch is processed, the Batcher's background goroutine exits and the channel
//     returned from [Done] is closed.
func New[Q, R any, F BatchProcessor[Q, R]](ctx context.Context, fn F, opts ...Option) *Batcher[Q, R] {
	cfg := processOptions(opts)
	b := &Batcher[Q, R]{
		cfg:                     cfg,
		addCh:                   make(chan batcherQ[Q, R]),
		addFromCh:               make(chan inputChannel[Q]),
		processBatch:            normalizeBatchProcessor[Q, R, F](fn),
		inFlight:                list.New(),
		runCtx:                  ctx,
		done:                    make(chan struct{}),
		deferMaxInFlightHardMet: true,
		deferMaxInFlightSoftMet: true,
	}
	if b.processBatch != nil {
		b.processedCh = make(chan *list.Element)
	}
	if b.cfg.deferMinAgeHard > 0 {
		b.deferMinAgeHardTimer = newTimer(b.cfg.clock)
	}
	if b.cfg.deferMinAgeSoft > 0 {
		b.deferMinAgeSoftTimer = newTimer(b.cfg.clock)
	}
	if b.cfg.deferRateLimitHard != nil {
		b.deferRateLimitHardTimer = newTimer(b.cfg.clock)
	}
	if b.cfg.deferRateLimitSoft != nil {
		b.deferRateLimitSoftTimer = newTimer(b.cfg.clock)
	}
	if b.cfg.constrainMaxAge < math.MaxInt64 {
		b.constrainMaxAgeTimer = newTimer(b.cfg.clock)
	}
	b.startNewBatch()
	go b.selectLoop()
	return b
}

// Done returns a channel that is closed when the Batcher has flushed the remaining input values
// after the context passed to [New] or [ToChan] is canceled.
func (b *Batcher[Q, R]) Done() <-chan struct{} {
	return b.done
}

func (b *Batcher[Q, R]) startNewBatch() {
	b.current = nil
	b.rChs = nil

	b.deferMinCountHardMet = b.cfg.deferMinCountHard <= 0
	b.deferMinCountSoftMet = b.cfg.deferMinCountSoft <= 0
	b.constrainMaxCountMet = b.cfg.constrainMaxCount <= 0
	b.deferMinAgeHardMet = false
	b.deferMinAgeHardTimer.Stop()
	b.deferMinAgeSoftMet = false
	b.deferMinAgeSoftTimer.Stop()
	b.deferRateLimitHardMet = true
	if b.cfg.deferRateLimitHard != nil {
		b.deferRateLimitHardMet = false
		now := b.cfg.clock.Now()
		r := b.cfg.deferRateLimitHard.ReserveN(now, 1)
		b.deferRateLimitHardTimer.Reset(r.DelayFrom(now))
	}
	b.deferRateLimitSoftMet = true
	if b.cfg.deferRateLimitSoft != nil {
		b.deferRateLimitSoftMet = false
		now := b.cfg.clock.Now()
		r := b.cfg.deferRateLimitSoft.ReserveN(now, 1)
		b.deferRateLimitSoftTimer.Reset(r.DelayFrom(now))
	}

	b.constrainMaxAgeMet = false
	b.constrainMaxAgeTimer.Stop()
}

func (b *Batcher[Q, R]) add(bq batcherQ[Q, R]) {
	b.current = append(b.current, bq.q)
	b.rChs = append(b.rChs, bq.rCh)
	if len(b.current) == 1 {
		b.constrainMaxAgeTimer.Reset(b.cfg.constrainMaxAge)
	}
	b.deferMinCountHardMet = len(b.current) >= b.cfg.deferMinCountHard
	b.deferMinCountSoftMet = len(b.current) >= b.cfg.deferMinCountSoft
	b.constrainMaxCountMet = len(b.current) >= b.cfg.constrainMaxCount
	b.deferMinAgeHardMet = b.cfg.deferMinAgeHard <= 0
	b.deferMinAgeHardTimer.Reset(b.cfg.deferMinAgeHard)
	b.deferMinAgeSoftMet = b.cfg.deferMinAgeSoft <= 0
	b.deferMinAgeSoftTimer.Reset(b.cfg.deferMinAgeSoft)
}

func (b *Batcher[Q, R]) selectLoop() {
	// Implementation note: This method uses reflect.Select because Go doesn't support select
	// statements with a variable number of cases (https://go.dev/issue/50324), and we want the user
	// to be able to register multiple input channels. An alternative approach would be to use a
	// separate goroutine to relay values from the multiple input channels into a single channel that
	// is read here in a normal select statement, but that would introduce unavoidable buffering
	// (https://go.dev/issue/60829).

	defer close(b.done)

	const (
		scRunCtxDone sel.CaseId = iota + 1
		scAdd
		scAddFrom
		scOut
		scProcessed
		scDeferMinAgeHard
		scDeferMinAgeSoft
		scDeferRateLimitHard
		scDeferRateLimitSoft
		scConstrainMaxAge
		_scNumStatic
	)
	ss := &sel.SelectStatement{}
	ss.SetChan(scRunCtxDone, b.runCtx.Done(), false)
	ss.SetChan(scAdd, b.addCh, false)
	ss.SetChan(scAddFrom, b.addFromCh, false)
	ss.SetChan(scOut, nil, true)
	ss.SetChan(scProcessed, b.processedCh, false)
	ss.SetChan(scDeferMinAgeHard, b.deferMinAgeHardTimer.Chan(), false)
	ss.SetChan(scDeferMinAgeSoft, b.deferMinAgeSoftTimer.Chan(), false)
	ss.SetChan(scDeferRateLimitHard, b.deferRateLimitHardTimer.Chan(), false)
	ss.SetChan(scDeferRateLimitSoft, b.deferRateLimitSoftTimer.Chan(), false)
	ss.SetChan(scConstrainMaxAge, b.constrainMaxAgeTimer.Chan(), false)

	for b.runCtx.Err() == nil || len(b.current) > 0 || b.inFlight.Len() > 0 {
		ss.SetChan(scOut, nil, true) // Intentionally nil until it is time to send a new batch.
		allDeferHardMet := b.deferMaxInFlightHardMet && b.deferMinCountHardMet &&
			b.deferMinAgeHardMet && b.deferRateLimitHardMet
		allDeferSoftMet := b.deferMaxInFlightSoftMet && b.deferMinCountSoftMet &&
			b.deferMinAgeSoftMet && b.deferRateLimitSoftMet
		anyConstraintMet := b.constrainCanceledMet || b.constrainMaxAgeMet || b.constrainMaxCountMet
		if allDeferHardMet && (allDeferSoftMet || anyConstraintMet) {
			// Time to (try to) dispatch the batch.
			if b.processBatch != nil {
				// The user provided a batch processing function. Give it the current batch and start a new
				// one.
				e := b.inFlight.PushBack(b.current)
				b.deferMaxInFlightHardMet = b.inFlight.Len() < b.cfg.deferMaxInFlightHard
				b.deferMaxInFlightSoftMet = b.inFlight.Len() < b.cfg.deferMaxInFlightSoft
				// b.startNewBatch clears b.current and b.rChs, so save them for use in the asynchronous
				// closure below.
				qs := b.current
				rChs := b.rChs
				b.startNewBatch()
				go func() {
					b.processBatch(b.runCtx, qs, rChs)
					// Remove e from b.inFlight in the main goroutine to avoid concurrency issues.
					b.processedCh <- e
				}()
			} else {
				// The user provided a channel for the batches. Try sending to it, but do not start a new
				// batch until the current batch is successfully sent. This allows additional incoming
				// values to accumulate if the receiver is backlogged.
				ss.SetChan(scOut, b.outCh, true)
				ss.SetSend(scOut, b.current)
			}
		}
		switch id, v, ok := ss.Select(); id {
		case scRunCtxDone:
			ss.SetChan(scRunCtxDone, nil, false)

			// Stop accepting new values.
			ss.SetChan(scAdd, nil, false)
			for i, done := range b.inputChDones {
				if done != nil {
					ss.SetChan(_scNumStatic+sel.CaseId(i), nil, false)
					close(done)
				}
			}
			b.inputChDones = nil

			b.constrainCanceledMet = true
			// Ignore any hard defer thresholds that cannot possibly be met with the values already in
			// b.current by pretending that the thresholds have already been met.
			b.deferMinCountHardMet = true
		case scAdd:
			bq := v.(batcherQ[Q, R])
			b.add(bq)
		case scAddFrom:
			ic := v.(inputChannel[Q])
			// Reuse a previously vacated slot if one exists. This prevents the accumulation of cruft if
			// channels are repeatedly added and drained. Calls to AddFrom are expected to be rare
			// relative to the number of values, so the O(N) overhead here is not worth optimizing away.
			var i int
			for i = 0; i < len(b.inputChDones); i++ {
				if b.inputChDones[i] == nil {
					break
				}
			}
			if i >= len(b.inputChDones) {
				b.inputChDones = append(b.inputChDones, ic.done)
			} else {
				b.inputChDones[i] = ic.done
			}
			id := _scNumStatic + sel.CaseId(i)
			ss.SetChan(id, ic.ch, false)
		case scOut:
			// This case only happens when dispatching directly to a user-supplied channel. Due to the
			// one-way nature of channels, there's no way to tell when the user code has finished
			// processing the batch, nor is there any way to obtain any return values from such processing
			// (so R should be struct{}). For the sake of callers to Add, the batch is treated as if
			// processing has already completed successfully with the zero value for its "return value".
			// There is no need to update b.inFlight or check in-flight thresholds/constraints.
			for _, rCh := range b.rChs {
				if rCh != nil {
					rCh <- batcherR[R]{*new(R), nil}
				}
			}
			b.startNewBatch()
		case scProcessed:
			e := v.(*list.Element)
			b.inFlight.Remove(e)
			b.deferMaxInFlightHardMet = b.inFlight.Len() < b.cfg.deferMaxInFlightHard
			b.deferMaxInFlightSoftMet = b.inFlight.Len() < b.cfg.deferMaxInFlightSoft
		case scDeferMinAgeHard:
			b.deferMinAgeHardMet = true
		case scDeferMinAgeSoft:
			b.deferMinAgeSoftMet = true
		case scDeferRateLimitHard:
			b.deferRateLimitHardMet = true
		case scDeferRateLimitSoft:
			b.deferRateLimitSoftMet = true
		case scConstrainMaxAge:
			b.constrainMaxAgeMet = true
		default:
			if id < _scNumStatic {
				panic(fmt.Errorf("unhandled select case %v", id))
			}
			i := int(id - _scNumStatic)
			if i >= len(b.inputChDones) || b.inputChDones[i] == nil {
				panic(fmt.Errorf("unexpected select case %v (input channel index %v)", id, i))
			}
			if !ok {
				ss.SetChan(id, nil, false)
				close(b.inputChDones[i])
				// Don't delete the entry (by moving later entries up one) because the index is used to
				// determine the select statement case ID.
				b.inputChDones[i] = nil
				break
			}
			q := v.(Q)
			b.add(batcherQ[Q, R]{q: q})
		}
	}
}

// A batcherQ is a query sent to the Batcher select loop.
type batcherQ[Q, R any] struct {
	q   Q                  // query value
	rCh chan<- batcherR[R] // response channel
}

// A batcherR is a response to a query.
type batcherR[R any] struct {
	r   R // response value
	err error
}

// Add enqueues a value for a future batch and blocks until the batch is processed or the provided
// context is canceled.
//
// If either ctx or the context passed to [New] or [ToChan] are canceled before the Batcher's
// background goroutine accepts the value, a zero value and the context's error are returned.
//
// If ctx is canceled before processing completes, a zero value and ctx's error are returned.
// (Canceling ctx does not abort processing; if that is needed, cancel the context passed to New
// or ToChan and ensure that the BatchProcessor function observes the state of its context
// argument.)
//
// Otherwise, the returned value and error are taken from the [BatchProcessor]'s return values.
func (b *Batcher[Q, R]) Add(ctx context.Context, q Q) (R, error) {
	rCh := make(chan batcherR[R])
	select {
	case <-ctx.Done():
		return *new(R), ctx.Err()
	case <-b.runCtx.Done():
		return *new(R), b.runCtx.Err()
	case b.addCh <- batcherQ[Q, R]{q: q, rCh: rCh}:
	}
	select {
	case <-ctx.Done():
		go func() { <-rCh }()
		return *new(R), ctx.Err()
	case br := <-rCh:
		return br.r, br.err
	}
}

// AddFrom registers ch with the Batcher. Values received from ch are added to batches in addition
// to any values passed to [Add] and any values received from another channel registered by a
// separate call to AddFrom. Values are received from ch until it is closed or the context passed
// to [New] or [ToChan] is canceled, at which point the returned channel is closed. The response and
// error return values that result from processing the values received from ch are not available.
func (b *Batcher[Q, R]) AddFrom(ch <-chan Q) <-chan struct{} {
	done := make(chan struct{})
	b.addFromCh <- inputChannel[Q]{ch: ch, done: done}
	return done
}

// ToChan returns a new [Batcher] that sends each batch to the output channel ch. The output channel
// is not closed when done. The Batcher will not send any new batches to the channel after the
// channel returned from [Done] is closed.
//
// The effects of canceling ctx are as described for [New].
//
// The MaxInFlight hard defer threshold is forced to 1, and a batch is considered to be in flight if
// ch is not ready to receive a batch.
func ToChan[Q any](ctx context.Context, ch chan<- []Q, opts ...Option) *Batcher[Q, struct{}] {
	b := New[Q, struct{}, func(context.Context, []Q) (struct{}, error)](ctx, nil, opts...)
	b.outCh = ch
	return b
}

// Chan is a convenience wrapper around [ToChan] and [Batcher.AddFrom]. It returns a new unbuffered
// channel that receives batches of values accumulated from the given input channel. Each batch
// includes every value received from the input channel (in the order they were received) since the
// previous batch was sent.
//
// If the input channel is closed, any previously received values that have not yet been dispatched
// are batched together and dispatched once all hard defer thresholds that can be met are met. After
// that last batch is dispatched, the returned channel is closed. The input channel must be closed
// to free resources and to ensure that all input values are sent to the returned channel.
func Chan[Q any](ch <-chan Q, opts ...Option) <-chan []Q {
	outCh := make(chan []Q)
	ctx, cancel := context.WithCancel(context.Background())
	b := ToChan(ctx, outCh, opts...)
	go func() {
		<-b.AddFrom(ch)
		cancel()
		<-b.Done()
		close(outCh)
	}()
	return outCh
}
