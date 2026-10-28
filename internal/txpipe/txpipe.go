// Package txpipe decouples the tx lane's read loop from cluster submission and
// turns per-transaction round trips into batched ones.
//
// The serial design — Handle blocking the lane on one POST /tx per object —
// couples throughput to the cluster's latency: 1/RTT per connection, and the
// RTT grows exactly when load does. Propagation exposes a batch endpoint
// (POST /txs, ≤1024 txs / ≤32 MiB per request) whose body is the same bare
// concatenated-transaction stream the lane itself carries, so the pipe
// accumulates lane objects into a contiguous body and ships one request per
// batch. Wire format conversion: none.
//
// # The batch contract
//
// Propagation processes a batch CONCURRENTLY with no in-batch ordering, and
// its handler documents the caller contract: a batch must not contain both a
// parent and any of its children, or the child races the parent and fails
// validation as missing-parent. The lane's arrival order has parents before
// children (self-chaining EF spends), so the pipe enforces the contract
// structurally: every transaction's input outpoints are walked (a cheap prefix
// scan, the same walk the framing codec already does), and a transaction that
// references a txid already in the open batch SEALS that batch first. Chains
// land in consecutive pipelined batches; independent transactions pack densely.
//
// # Failure classification
//
//   - 200: every transaction in the batch was accepted (or already known).
//   - 500 "Failed to process transactions": SOME failed; the others were still
//     processed. The body carries one error line per failure including the
//     txid, so failures are re-attributed to batch members and retried as one
//     further batch — missing-parent resolves once the parent lands, which is
//     bounded-retry, not fail-forever.
//   - 429: the endpoint's rate limiter refused the request before reading it.
//     No member was processed, but splitting the batch into per-member retries
//     would multiply the request rate by the batch size against the very
//     limiter that just refused us, so a rate-limited batch backs off and is
//     retried WHOLE, and never enters per-member salvage.
//   - anything else / transport error: the whole batch is retried once on the
//     next endpoint, then counted failed.
//
// Bytes handed to Enqueue are copied immediately: lane slices alias the
// reader's buffer and are only valid until the next read.
package txpipe

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/lightwebinc/teranode-bridge/hashid"
	"github.com/lightwebinc/teranode-bridge/internal/obs"
)

// Config sizes the pipe. Zero values pick defaults suited to a single
// propagation backend on the same LAN.
type Config struct {
	// Endpoints are propagation HTTP base URLs, round-robined per batch.
	Endpoints []string
	// BatchTxs seals a batch at this many transactions (server cap 1024).
	BatchTxs int
	// BatchBytes seals a batch at this body size (server cap 32 MiB).
	BatchBytes int
	// Linger seals a non-empty batch this long after its first transaction,
	// bounding added latency when the lane is quiet.
	Linger time.Duration
	// Inflight bounds concurrent batch requests. Throughput ≈
	// Inflight × BatchTxs / RTT.
	Inflight int
	// RetryAttempts bounds per-transaction retries for failures that resolve
	// with time (missing parent). 0 disables retry.
	RetryAttempts int
	// Timeout is the per-request deadline.
	Timeout time.Duration
	// Queue is the Enqueue buffer depth; a full queue blocks the lane, which
	// is the backpressure that keeps memory bounded.
	Queue int
	// Builders is the number of parallel batch builders (power of two, ≤16).
	// One builder is a serialization point around 400k tx/s; transactions are
	// routed to builders by txid, so any single builder's batch still honours
	// the parent/child contract for the pairs it sees, and cross-builder pairs
	// fall to the retry backstop exactly like cross-connection pairs already do
	// in the fabric (round-robin delivery sprays chain members across
	// connections regardless).
	Builders int
}

// maxServerBatchTxs is the largest batch propagation will ACCEPT, which is one
// less than the limit its constant names. Server.go checks
// `totalNrTransactions >= maxTransactionsPerRequest` (1024) at the TOP of the
// read loop, so the 1024th transaction is read, dispatched and processed, and
// only then does the loop re-enter, trip the check and answer 400. A batch of
// exactly 1024 is therefore fully applied AND reported as failed — the worst
// of both, since every member would be resubmitted as a duplicate. 1023 is the
// real cap.
const maxServerBatchTxs = 1023

// rateLimitBackoff is the whole-batch retry ladder for 429. Propagation's HTTP
// limiter is per source IP AND per endpoint (Server.go: HTTPRateLimit), so a
// bridge saturating one endpoint is throttled while a sibling endpoint is
// still free — hence the failover attempt happens first and this ladder only
// covers the case where every endpoint is throttled at once. Var, not const,
// so tests can shorten it.
var rateLimitBackoff = []time.Duration{20 * time.Millisecond, 80 * time.Millisecond, 320 * time.Millisecond}

func (c *Config) defaults() {
	if c.BatchTxs <= 0 {
		c.BatchTxs = 512
	}
	if c.BatchTxs > maxServerBatchTxs {
		c.BatchTxs = maxServerBatchTxs
	}
	if c.BatchBytes <= 0 {
		c.BatchBytes = 8 << 20
	}
	if c.BatchBytes > 30<<20 {
		c.BatchBytes = 30 << 20 // stay under the server's 32 MiB cap
	}
	if c.Linger <= 0 {
		c.Linger = 2 * time.Millisecond
	}
	if c.Inflight <= 0 {
		c.Inflight = 4
	}
	if c.Timeout <= 0 {
		c.Timeout = 30 * time.Second
	}
	if c.Queue <= 0 {
		c.Queue = 8192
	}
	switch {
	case c.Builders < 1:
		c.Builders = 4
	case c.Builders > 16:
		c.Builders = 16
	}
	// round down to a power of two so routing is a mask
	for c.Builders&(c.Builders-1) != 0 {
		c.Builders--
	}
}

type job struct {
	raw []byte // owned copy
	id  hashid.Hash
}

type batch struct {
	body bytes.Buffer
	jobs []job
	ids  map[hashid.Hash]struct{}
	born time.Time
}

// Pipe accepts transactions from the lane and submits them in batches.
type Pipe struct {
	cfg    Config
	client *http.Client
	log    *slog.Logger

	ins []chan []job
	rr  atomic.Uint64 // round-robin builder choice for Batch sends

	// queued counts transactions sent to a builder and not yet taken off its
	// queue (the queues hold runs, so their lengths do not).
	queued atomic.Int64
	sem    chan struct{}
	wg     sync.WaitGroup
	next   atomic.Uint64
	rtxid  *regexp.Regexp

	// pool recycles batch bodies, job slices and id sets once a submission
	// settles. Without it every batch regrew its body by doubling, which at a
	// million transactions a second allocated twice the payload again.
	pool sync.Pool

	// counters, read via Stats
	enqueued, accepted, rejected, failed     atomic.Uint64
	retried, retryAccepted, unattributed     atomic.Uint64
	batches, rateLimited                     atomic.Uint64
	sealSize, sealBytes, sealLinger, sealDep atomic.Uint64
}

// New returns a pipe. Call Run to start it.
func New(cfg Config, log *slog.Logger) (*Pipe, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("txpipe: no propagation endpoints configured")
	}
	cfg.defaults()
	eps := make([]string, 0, len(cfg.Endpoints))
	for _, e := range cfg.Endpoints {
		eps = append(eps, strings.TrimRight(e, "/"))
	}
	cfg.Endpoints = eps

	tr := http.DefaultTransport.(*http.Transport).Clone()
	// One idle conn per in-flight batch plus retry headroom, per endpoint.
	tr.MaxIdleConnsPerHost = cfg.Inflight + 8
	// And a HARD cap on live connections, not just idle ones. Without it a
	// retry storm opens a socket per rejected transaction and exhausts the
	// host's ephemeral ports — observed as "connect: cannot assign requested
	// address" against a cluster that was answering normally.
	tr.MaxConnsPerHost = cfg.Inflight + 8
	tr.IdleConnTimeout = 90 * time.Second

	// Propagation extracts trace context from request headers. Without the
	// otelhttp round-tripper every transaction the bridge forwards begins a NEW
	// root span cluster-side, so the wide-area leg that carried it is missing
	// from the trace. With tracing disabled this wraps a no-op provider.
	rt := otelhttp.NewTransport(tr)

	// The queues carry runs of transactions (a Batch from a lane session, at
	// most BatchCap each, or a single one from Enqueue). Capacity is counted
	// in runs so that, at saturation where every run is full, the pipe holds
	// about cfg.Queue transactions before backpressure reaches the lane.
	ins := make([]chan []job, cfg.Builders)
	for i := range ins {
		ins[i] = make(chan []job, max(4, cfg.Queue/(cfg.Builders*BatchCap)))
	}
	return &Pipe{
		cfg:    cfg,
		client: &http.Client{Timeout: cfg.Timeout, Transport: rt},
		log:    log,
		ins:    ins,
		sem:    make(chan struct{}, cfg.Inflight),
		rtxid:  regexp.MustCompile(`\[([0-9a-f]{64})\]`),
	}, nil
}

// Enqueue hands one whole transaction to the pipe. The bytes are copied before
// return; the caller may reuse the slice. Blocks when the queue is full — that
// backpressure propagates to the lane's TCP window, which is the design.
func (p *Pipe) Enqueue(ctx context.Context, raw []byte, id hashid.Hash) error {
	return p.EnqueueOwned(ctx, append([]byte(nil), raw...), id)
}

// EnqueueOwned is Enqueue without the copy: ownership of raw transfers to the
// pipe and the caller must never mutate it again. The tx hot path makes ONE
// immutable copy per object and shares it between the cache and the pipe.
func (p *Pipe) EnqueueOwned(ctx context.Context, raw []byte, id hashid.Hash) error {
	return p.send(ctx, p.ins[int(id[1])&(p.cfg.Builders-1)], []job{{raw: raw, id: id}})
}

// BatchCap is the most transactions a Batch carries in one send.
const BatchCap = 64

// Batch is a run of owned transactions handed to the pipe in one channel
// send: the tx lane fills one per connection and flushes it before it blocks
// on the network, so the per-transaction cost of the hand-off (a channel send,
// and often a builder wake-up) is paid once per run instead. A Batch is not
// safe for concurrent use.
type Batch struct {
	p    *Pipe
	jobs []job
}

// NewBatch returns an empty batch bound to this pipe.
func (p *Pipe) NewBatch() *Batch { return &Batch{p: p} }

// Add appends one transaction; ownership of raw transfers to the pipe, as with
// EnqueueOwned. It reports whether the batch is full and must be flushed.
func (b *Batch) Add(raw []byte, id hashid.Hash) (full bool) {
	if b.jobs == nil {
		b.jobs = b.p.getJobs()
	}
	b.jobs = append(b.jobs, job{raw: raw, id: id})
	return len(b.jobs) >= BatchCap
}

// Len reports how many transactions are waiting in the batch.
func (b *Batch) Len() int { return len(b.jobs) }

// Flush hands the batch's transactions to one builder (round robin), blocking
// while its queue is full: that backpressure reaches the lane's TCP window,
// as with EnqueueOwned. The batch is empty and reusable afterwards.
func (b *Batch) Flush(ctx context.Context) error {
	if len(b.jobs) == 0 {
		return nil
	}
	js := b.jobs
	b.jobs = nil
	p := b.p
	return p.send(ctx, p.ins[int(p.rr.Add(1))&(p.cfg.Builders-1)], js)
}

// send queues one run of transactions on a builder. Ownership of js moves to
// the builder, which returns it to the pool once consumed.
func (p *Pipe) send(ctx context.Context, in chan []job, js []job) error {
	// Fast path: an uncontended buffered send skips selectgo entirely, which
	// is measurable at hundreds of thousands of sends per second.
	n := len(js)
	select {
	case in <- js:
		p.enqueued.Add(uint64(n))
		p.queued.Add(int64(n))
		return nil
	default:
	}
	select {
	case in <- js:
		p.enqueued.Add(uint64(n))
		p.queued.Add(int64(n))
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

var jobPool = sync.Pool{New: func() any { s := make([]job, 0, BatchCap); return &s }}

func (p *Pipe) getJobs() []job { return (*jobPool.Get().(*[]job))[:0] }

// putJobs returns a consumed run to the pool. Single-transaction runs from
// EnqueueOwned are left to the collector.
func (p *Pipe) putJobs(js []job) {
	if cap(js) < BatchCap {
		return
	}
	clear(js) // drop the references to transaction bytes
	js = js[:0]
	jobPool.Put(&js)
}

// Run consumes the queues until ctx is cancelled, then flushes what it holds.
func (p *Pipe) Run(ctx context.Context) error {
	var bwg sync.WaitGroup
	for _, in := range p.ins {
		bwg.Add(1)
		go func(in chan []job) {
			defer bwg.Done()
			p.runBuilder(ctx, in)
		}(in)
	}
	bwg.Wait()
	return nil
}

// runBuilder is one builder's accumulate/seal loop over its own queue.
func (p *Pipe) runBuilder(ctx context.Context, in chan []job) {
	cur := p.newBatch()
	linger := time.NewTimer(time.Hour)
	linger.Stop()
	defer linger.Stop()

	seal := func(reason *atomic.Uint64) {
		if len(cur.jobs) == 0 {
			return
		}
		reason.Add(1)
		b := cur
		cur = p.newBatch()
		linger.Stop()
		select {
		case <-linger.C: // drain a fired-but-unread timer
		default:
		}
		p.sem <- struct{}{}
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			defer func() { <-p.sem }()
			p.submit(ctx, b)
			p.recycle(b)
		}()
	}

	add := func(j job) {
		if p.dependsOnBatch(j.raw, cur.ids) {
			// The documented /txs contract: never a parent and its child
			// in one request. Seal the parent's batch; the child opens the
			// next. Chains pipeline across batches instead of failing.
			seal(&p.sealDep)
		}
		if len(cur.jobs) == 0 {
			cur.born = time.Now()
			linger.Reset(p.cfg.Linger)
		}
		cur.body.Write(j.raw)
		cur.jobs = append(cur.jobs, j)
		cur.ids[j.id] = struct{}{}
		switch {
		case len(cur.jobs) >= p.cfg.BatchTxs:
			seal(&p.sealSize)
		case cur.body.Len() >= p.cfg.BatchBytes:
			seal(&p.sealBytes)
		}
	}

	addRun := func(js []job) {
		p.queued.Add(-int64(len(js)))
		for _, j := range js {
			add(j)
		}
		p.putJobs(js)
	}

	for {
		select {
		case js := <-in:
			addRun(js)
			// Greedy drain: one selectgo wake services everything already
			// queued, so the per-run cost of this loop is a plain channel
			// receive, not a select.
		drain:
			for {
				select {
				case js := <-in:
					addRun(js)
				default:
					break drain
				}
			}

		case <-linger.C:
			seal(&p.sealLinger)

		case <-ctx.Done():
			// Drain what is already queued, seal, and wait for in-flight
			// submissions so accepted counts are honest at shutdown.
			for {
				select {
				case js := <-in:
					p.queued.Add(-int64(len(js)))
					for _, j := range js {
						cur.body.Write(j.raw)
						cur.jobs = append(cur.jobs, j)
					}
				default:
					seal(&p.sealLinger)
					p.wg.Wait()
					return
				}
			}
		}
	}
}

func (p *Pipe) newBatch() *batch {
	if b, ok := p.pool.Get().(*batch); ok {
		return b
	}
	b := &batch{
		ids:  make(map[hashid.Hash]struct{}, p.cfg.BatchTxs),
		jobs: make([]job, 0, p.cfg.BatchTxs),
	}
	b.body.Grow(min(p.cfg.BatchBytes, p.cfg.BatchTxs*bodyHintPerTx))
	return b
}

// bodyHintPerTx sizes a fresh batch body; a pooled body keeps whatever
// capacity it grew to.
const bodyHintPerTx = 512

// recycle returns a settled batch to the pool. Nothing may reference b, its
// jobs or its body afterwards: submit copies what a retry keeps. A body that
// grew far past the hint (very large transactions) is left to the collector
// so the pool cannot pin it.
func (p *Pipe) recycle(b *batch) {
	if b.body.Cap() > 4*p.cfg.BatchTxs*bodyHintPerTx {
		return
	}
	clear(b.jobs) // drop the references to transaction bytes
	b.jobs = b.jobs[:0]
	clear(b.ids)
	b.body.Reset()
	b.born = time.Time{}
	p.pool.Put(b)
}

// submit ships one batch and classifies the outcome.
func (p *Pipe) submit(ctx context.Context, b *batch) {
	p.batches.Add(1)
	n := uint64(len(b.jobs))

	// Endpoint refusals get one failover attempt each; rate-limit refusals are
	// transient and get their own bounded ladder without consuming that budget.
	throttled := 0
	for attempt := 0; attempt-throttled < 2; attempt++ {
		ep := p.cfg.Endpoints[int(p.next.Add(1)-1)%len(p.cfg.Endpoints)]
		status, body, err := p.post(ctx, ep+"/txs", b.body.Bytes())
		switch {
		case err != nil:
			p.log.Warn("txpipe: batch transport error", "endpoint", ep, "txs", n, "err", err)
			continue // next endpoint

		case status == http.StatusOK:
			p.accepted.Add(n)
			return

		case strings.HasPrefix(body, partialPrefix) && (status == http.StatusInternalServerError || p.namesMember(body)):
			// Partial failure, WHATEVER the status: propagation validates every
			// member independently and accepts the good ones, then reports the
			// batch with the first client error among the failures (422 for a
			// missing parent, 409 for a conflict, 400 for an invalid tx), or 500
			// when a server fault is among them. Only a 500 used to be read this
			// way; a 422 fell to the refusal path below, which re-sent all 512
			// members six times over the retry ladder and then booked every one
			// of them rejected. One orphan in a batch cost the cluster ~3000
			// re-validations of accepted transactions, at exactly the moment it
			// was already behind (measured: a 20k tx/s lab run collapsed into
			// 422 storms within seconds of its first orphan).
			p.settlePartial(ctx, b, body)
			return

		case status == http.StatusTooManyRequests:
			// Refused by the endpoint's limiter before the body was read, so
			// no member was processed. Per-member salvage would turn one
			// refused batch into len(b.jobs) requests aimed at the limiter
			// that just refused us, so back off and retry the batch WHOLE.
			p.rateLimited.Add(1)
			if throttled >= len(rateLimitBackoff) {
				p.log.Warn("txpipe: batch still rate-limited after backoff ladder",
					"endpoint", ep, "txs", n)
				p.failed.Add(n)
				return
			}
			select {
			case <-time.After(rateLimitBackoff[throttled]):
			case <-ctx.Done():
				p.failed.Add(n)
				return
			}
			throttled++
			continue

		default:
			p.log.Warn("txpipe: batch refused", "endpoint", ep, "status", status,
				"txs", n, "body", firstLine(body))
			continue
		}
	}
	// Both endpoints refused the whole batch. Every member is known
	// undelivered, so salvage them as one retry batch rather than writing off n
	// transactions on one bad request — a batch-shaped fault (an oversized
	// body, one poisonous member) does not mean the others are unacceptable.
	if p.cfg.RetryAttempts > 0 {
		p.log.Warn("txpipe: batch failed on all endpoints; salvaging as one retry batch", "txs", n)
		jobs := append([]job(nil), b.jobs...) // b is recycled when submit returns
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.retryBatch(ctx, jobs)
		}()
		return
	}
	p.failed.Add(n)
}

// partialPrefix opens propagation's per-member failure report on /txs.
const partialPrefix = "Failed to process transactions"

// isMissingParent reports whether a failure line is a missing parent
// ("TX_MISSING_PARENT (34): [ProcessTransaction][<txid>] missing parent tx"),
// by its code or its message. It is the one failure that resolves with time
// (the parent lands), so it is the only one worth retrying; any other named
// failure is final.
func isMissingParent(line string) bool {
	return strings.Contains(line, "MISSING_PARENT") || strings.Contains(strings.ToLower(line), "missing parent")
}

// isRetryable reports whether a failure line describes a failure that
// resolves with time, so its transaction goes back on the retry ladder: a
// missing parent, or THRESHOLD_EXCEEDED, which propagation answers when block
// assembly sheds load. That transaction is stored but not yet in a block
// template, and a resubmit re-drives the hand-off; Teranode itself classifies
// it as a retryable overload (503 on POST /tx). Every other named failure is
// final.
func isRetryable(line string) bool {
	return isMissingParent(line) || strings.Contains(line, "THRESHOLD_EXCEEDED")
}

// namesMember reports whether a partial-failure body attributes at least one
// line to a transaction. One that names nobody cannot be settled per member, so
// it keeps the whole-batch handling.
func (p *Pipe) namesMember(body string) bool {
	for _, line := range strings.Split(body, "\n") {
		if _, ok := p.subjectTxID(line); ok {
			return true
		}
	}
	return false
}

// settlePartial parses a partial-failure body's error lines, retries the named
// txids that are waiting on a parent, books other named failures rejected, and
// counts everything else accepted.
//
// Attribution is not guaranteed: an error line may name no batch member (a
// parse error before any txid was read, or a message quoting a foreign hash).
// Counting the remainder accepted would then overstate delivery, so any
// unattributed error line is counted as an UNATTRIBUTED failure and subtracted
// from the accepted remainder. The counter exists so an operator can see that
// the batch's outcome was partly unknowable rather than silently good.
func (p *Pipe) settlePartial(ctx context.Context, b *batch, body string) {
	mine := make(map[string]struct{}, len(b.jobs))
	for _, j := range b.jobs {
		mine[j.id.Display()] = struct{}{}
	}

	failedIDs := make(map[string]bool) // txid -> retryable (missing parent, threshold exceeded)
	unattributed := 0
	for _, line := range strings.Split(body, "\n") {
		id, ok := p.subjectTxID(line)
		if !ok {
			continue // not a per-transaction error line
		}
		if _, ours := mine[id]; ours {
			failedIDs[id] = isRetryable(line)
			continue
		}
		unattributed++
	}

	var toRetry []job
	final := 0
	for _, j := range b.jobs {
		retryable, failed := failedIDs[j.id.Display()]
		switch {
		case !failed:
		case retryable:
			toRetry = append(toRetry, j)
		default:
			final++ // invalid, conflicting, double spend: waiting will not help
		}
	}
	if final > 0 {
		p.rejected.Add(uint64(final))
	}
	if unattributed > 0 {
		p.unattributed.Add(uint64(unattributed))
		p.log.Warn("txpipe: batch error lines name no batch member; outcome unknown for that many txs",
			"count", unattributed, "batch_txs", len(b.jobs), "body", firstLine(body))
	}
	// Never let the unknown count inflate accepted.
	ok := len(b.jobs) - len(toRetry) - final - unattributed
	if ok < 0 {
		ok = 0
	}
	p.accepted.Add(uint64(ok))

	if p.cfg.RetryAttempts <= 0 || len(toRetry) == 0 {
		// Error lines that matched no batch member (parse errors, foreign
		// txids in messages) have no job to retry; they are counted against
		// the batch as rejected only when they named one of ours.
		p.rejected.Add(uint64(len(toRetry)))
		return
	}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.retryBatch(ctx, toRetry)
	}()
}

// subjectTxID returns the transaction an error line is ABOUT.
//
// Matching every 64-hex token in the body is wrong, and wrong in the direction
// that corrupts accounting: propagation's messages quote hashes that are not
// the subject — "[ProcessTransaction][<txid>] duplicate input found:
// <prevTxID>:<vout>" carries two. The prevout belongs to a transaction that is
// almost never a batch member, so every such line would book one phantom
// unattributed failure and subtract a real acceptance. The subject is the one
// the error convention BRACKETS; a prevout or a quoted body never is.
func (p *Pipe) subjectTxID(line string) (string, bool) {
	m := p.rtxid.FindStringSubmatch(line)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// retryBatch resubmits everything a batch left undelivered as ONE further
// batch, under the same inflight semaphore a first submission takes.
//
// The shape this replaces — one goroutine and one singleton POST /tx per
// rejected transaction, outside the semaphore — is what turned a partial
// failure into a throughput collapse. Measured against this cluster:
// single-transaction retries were 98.9% of propagation's requests and 97.7%
// of its handler seconds, for a 0.074% recovery yield, and being unbounded
// they also exhausted the bridge's ephemeral ports. Retrying as a batch keeps
// request count proportional to BATCHES rather than to transactions, and the
// semaphore bounds what the cluster is asked to hold either way.
//
// Batching a retry is safe against the /txs parent-child contract: this set is
// a subset of one batch that was already dependency-sealed, so it cannot hold
// a parent and its child. Missing-parent (422) resolves when the PARENT's
// batch lands — a different request — which is why waiting and resubmitting
// works at all, and why it never needed to be one request per transaction.
func (p *Pipe) retryBatch(ctx context.Context, jobs []job) {
	if len(jobs) == 0 {
		return
	}
	p.retried.Add(uint64(len(jobs)))
	// The ladder has to outlast the slowest parent, not the typical one: under a
	// 500 tx/s soak a burst of 76 children (in 13 batches) stayed 422 past the
	// old 25/100/400 ms ladder and was booked rejected, losing every one of them.
	// Waiting longer costs a goroutine; giving up costs the transaction.
	delays := []time.Duration{25 * time.Millisecond, 100 * time.Millisecond, 400 * time.Millisecond, 1600 * time.Millisecond, 3200 * time.Millisecond}
	pending := jobs

	for attempt := 0; attempt < p.cfg.RetryAttempts && len(pending) > 0; attempt++ {
		select {
		case <-time.After(delays[min(attempt, len(delays)-1)]):
		case <-ctx.Done():
			p.failed.Add(uint64(len(pending)))
			return
		}

		var body bytes.Buffer
		for _, j := range pending {
			body.Write(j.raw)
		}

		select {
		case p.sem <- struct{}{}:
		case <-ctx.Done():
			p.failed.Add(uint64(len(pending)))
			return
		}
		ep := p.cfg.Endpoints[int(p.next.Add(1)-1)%len(p.cfg.Endpoints)]
		status, respBody, err := p.post(ctx, ep+"/txs", body.Bytes())
		<-p.sem

		switch {
		case err != nil:
			continue // transport fault; the ladder tries the other endpoint

		case status == http.StatusOK:
			p.retryAccepted.Add(uint64(len(pending)))
			p.accepted.Add(uint64(len(pending)))
			return

		case strings.HasPrefix(respBody, partialPrefix) && (status == http.StatusInternalServerError || p.namesMember(respBody)):
			// Any status: only the transactions the body still NAMES are
			// outstanding; the rest of the retry batch landed and are booked as
			// recovered. Of the named, only those waiting on a parent stay in
			// the ladder; the rest are final and leave it now.
			named := make(map[string]bool)
			for _, line := range strings.Split(respBody, "\n") {
				if id, ok := p.subjectTxID(line); ok {
					named[id] = isRetryable(line)
				}
			}
			next := pending[:0:0]
			final := 0
			for _, j := range pending {
				retryable, bad := named[j.id.Display()]
				switch {
				case !bad:
				case retryable:
					next = append(next, j)
				default:
					final++
				}
			}
			if ok := len(pending) - len(next) - final; ok > 0 {
				p.retryAccepted.Add(uint64(ok))
				p.accepted.Add(uint64(ok))
			}
			if final > 0 {
				p.rejected.Add(uint64(final))
			}
			pending = next

		case status == http.StatusTooManyRequests:
			p.rateLimited.Add(1)
			continue // the ladder's own wait is the backoff

		case status == http.StatusUnprocessableEntity:
			continue // every member still waiting on a parent

		default:
			p.rejected.Add(uint64(len(pending)))
			return
		}
	}

	if len(pending) > 0 {
		p.rejected.Add(uint64(len(pending)))
		p.log.Warn("txpipe: retries exhausted", "txs", len(pending))
	}
}

// post issues one batch submission and times it.
//
// The histogram is labelled by how the round trip ended, because "slow" and
// "refusing" are different faults with the same counter signature: an endpoint
// that 429s in a millisecond and one that accepts in two seconds both move
// teranode_bridge_txpipe_batches_total at the same rate. Timing is measured around the
// whole exchange including the body read, since that is what the caller waits
// on and what holds an in-flight slot.
func (p *Pipe) post(ctx context.Context, url string, body []byte) (int, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	start := time.Now()
	resp, err := p.client.Do(req)
	if err != nil {
		obs.Since(obs.SubmitDuration, start, "failed")
		return 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	outcome := "failed"
	switch {
	case resp.StatusCode == http.StatusOK:
		outcome = "ok"
		obs.Stamp(obs.LastSubmitTime)
	case resp.StatusCode == http.StatusTooManyRequests:
		outcome = "rate_limited"
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		outcome = "rejected"
	}
	obs.Since(obs.SubmitDuration, start, outcome)
	return resp.StatusCode, string(rb), nil
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	if len(s) > 200 {
		return s[:200]
	}
	return s
}

// dependsOnBatch walks tx's inputs and reports whether any prevout txid is in
// ids. The walk mirrors the objfmt codec's structure scan over BRC-12 raw and
// BRC-30 EF transactions; a malformed tx reports false and is left for the
// server to reject.
func (p *Pipe) dependsOnBatch(tx []byte, ids map[hashid.Hash]struct{}) bool {
	hit := false
	_ = inputRefs(tx, func(prev hashid.Hash) {
		if _, ok := ids[prev]; ok {
			hit = true
		}
	})
	return hit
}

// inputRefs calls fn with each input's prevout txid (wire order).
func inputRefs(tx []byte, fn func(hashid.Hash)) error {
	off := 4 // version
	if len(tx) < 10 {
		return fmt.Errorf("txpipe: short tx")
	}
	// BRC-30 EF marker: 0x0000000000EF where the input count would start.
	ef := tx[4] == 0 && tx[5] == 0 && tx[6] == 0 && tx[7] == 0 && tx[8] == 0 && tx[9] == 0xEF
	if ef {
		off += 6
	}
	inCount, n, err := varInt(tx, off)
	if err != nil || inCount == 0 {
		return fmt.Errorf("txpipe: bad input count")
	}
	off += n
	for i := uint64(0); i < inCount; i++ {
		if off+36 > len(tx) {
			return fmt.Errorf("txpipe: truncated input")
		}
		var h hashid.Hash
		copy(h[:], tx[off:off+32])
		fn(h)
		off += 36 // prev txid + index
		sLen, n, err := varInt(tx, off)
		if err != nil {
			return err
		}
		off += n + int(sLen) + 4 // unlocking script + sequence
		if ef {
			if off+8 > len(tx) {
				return fmt.Errorf("txpipe: truncated EF input")
			}
			off += 8 // spent value
			lLen, n, err := varInt(tx, off)
			if err != nil {
				return err
			}
			off += n + int(lLen) // spent locking script
		}
		if off > len(tx) {
			return fmt.Errorf("txpipe: input overruns tx")
		}
	}
	return nil
}

func varInt(b []byte, off int) (uint64, int, error) {
	if off >= len(b) {
		return 0, 0, fmt.Errorf("txpipe: short varint")
	}
	switch v := b[off]; {
	case v < 0xfd:
		return uint64(v), 1, nil
	case v == 0xfd:
		if off+3 > len(b) {
			return 0, 0, fmt.Errorf("txpipe: short varint16")
		}
		return uint64(b[off+1]) | uint64(b[off+2])<<8, 3, nil
	case v == 0xfe:
		if off+5 > len(b) {
			return 0, 0, fmt.Errorf("txpipe: short varint32")
		}
		return uint64(b[off+1]) | uint64(b[off+2])<<8 | uint64(b[off+3])<<16 | uint64(b[off+4])<<24, 5, nil
	default:
		if off+9 > len(b) {
			return 0, 0, fmt.Errorf("txpipe: short varint64")
		}
		var x uint64
		for i := 0; i < 8; i++ {
			x |= uint64(b[off+1+i]) << (8 * i)
		}
		return x, 9, nil
	}
}

// Stats is a snapshot for logging and metrics.
type Stats struct {
	Enqueued, Accepted, Rejected, Failed     uint64
	Retried, RetryAccepted, Unattributed     uint64
	Batches, RateLimited                     uint64
	SealSize, SealBytes, SealLinger, SealDep uint64
	Queue                                    int
}

// Stats returns a snapshot of the pipe's counters.
func (p *Pipe) Stats() Stats {
	q := int(max(p.queued.Load(), 0))
	return Stats{
		Enqueued: p.enqueued.Load(), Accepted: p.accepted.Load(),
		Rejected: p.rejected.Load(), Failed: p.failed.Load(),
		Retried: p.retried.Load(), RetryAccepted: p.retryAccepted.Load(),
		Unattributed: p.unattributed.Load(),
		Batches:      p.batches.Load(), RateLimited: p.rateLimited.Load(),
		SealSize: p.sealSize.Load(), SealBytes: p.sealBytes.Load(),
		SealLinger: p.sealLinger.Load(), SealDep: p.sealDep.Load(),
		Queue: q,
	}
}
