// Package retrieval serves the pulls a Teranode cluster makes after the bridge
// announces an object — the half of the announce shim that makes a pushed object
// look, to the cluster, exactly like one fetched from a peer's Asset service.
//
// It implements the subset of that API which subtreevalidation and
// blockvalidation actually call, and nothing else. Two rules shape the
// implementation:
//
//   - Never answer 200 with an empty or wrong body. An empty subtree body is an
//     explicit error in the cluster, and a wrong body fails a root-hash check
//     that would otherwise be a silent corruption. Unknown object => 404.
//
//   - Never answer 5xx for something we simply do not have. On the block path a
//     server-fault status is classified as recoverable, so the cluster does not
//     commit the Kafka offset and redelivers forever; 404 ends it cleanly.
package retrieval

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lightwebinc/shard-common/objfmt"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/lightwebinc/teranode-bridge/internal/cache"
	"github.com/lightwebinc/teranode-bridge/internal/hashid"
	"github.com/lightwebinc/teranode-bridge/internal/obs"
	"github.com/lightwebinc/teranode-bridge/internal/tnwire"
)

// Store is the read side of the object cache the ingest plane fills.
type Store interface {
	Get(key cache.Key) (body []byte, class string, ok bool)
	Has(key cache.Key) bool
}

// TxStore resolves a transaction by its txid, for serving subtree members.
type TxStore interface {
	Get(key cache.Key) (body []byte, class string, ok bool)
}

// Config describes the retrieval plane.
type Config struct {
	// Listen is the address to bind (e.g. "[2001:db8:3f::1]:9145").
	Listen string
	// APIPrefix must match what is announced. Teranode concatenates the
	// announced base URL with "/subtree/{hash}" verbatim, and its own asset
	// mounts everything under /api/v1 — so the announced URL carries the prefix
	// and we must answer on it.
	APIPrefix string
}

// Server answers cluster pulls out of the caches.
type Server struct {
	cfg      Config
	subtrees Store
	blocks   Store
	txs      TxStore
	log      *slog.Logger

	// firstPull closes the announce→pull loop: the announce side records when
	// the cluster was told, this side records when it acted.
	firstPull *obs.FirstPull

	srv *http.Server

	bound atomic.Bool

	hitSubtree, hitSubtreeData, hitTxs, hitBlock atomic.Uint64
	miss, errs                                   atomic.Uint64
	missRoute, missChainSync                     atomic.Uint64
}

// New builds the server. subtrees/blocks/txs may be the same cache instance.
func New(cfg Config, subtrees, blocks Store, txs TxStore, log *slog.Logger) *Server {
	if cfg.APIPrefix == "" {
		cfg.APIPrefix = "/api/v1"
	}
	cfg.APIPrefix = "/" + strings.Trim(cfg.APIPrefix, "/")
	return &Server{cfg: cfg, subtrees: subtrees, blocks: blocks, txs: txs, log: log}
}

// Listening reports whether the retrieval plane has its listener open. It gates
// readiness: an announcement already sent points at this socket, and Teranode
// does not retry a failed subtree fetch, so a bridge that announced and then
// cannot answer has lost that object rather than delayed it.
func (s *Server) Listening() bool { return s.bound.Load() }

// SetFirstPull attaches the announce→first-pull tracker.
func (s *Server) SetFirstPull(f *obs.FirstPull) { s.firstPull = f }

// BaseURL is the value to announce: the address the cluster will dial, with the
// API prefix and no trailing slash (a trailing slash would produce "//subtree/").
func (s *Server) BaseURL(advertise string) string {
	return strings.TrimRight(advertise, "/") + s.cfg.APIPrefix
}

// Handler builds the route table the cluster pulls against. Separated from
// Serve so the contract can be exercised without binding a socket.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	p := s.cfg.APIPrefix

	mux.HandleFunc("GET "+p+"/subtree/{hash}", s.timed("subtree", s.getSubtree))
	mux.HandleFunc("GET "+p+"/subtree_data/{hash}", s.timed("subtree_data", s.getSubtreeData))
	mux.HandleFunc("POST "+p+"/subtree/{hash}/txs", s.timed("txs", s.postSubtreeTxs))
	mux.HandleFunc("GET "+p+"/block/{hash}", s.timed("block", s.getBlock))

	// One caller in the cluster appends the API prefix to an already-prefixed
	// base URL, producing /api/v1/api/v1/subtree_data/{hash}. Serving the alias
	// costs nothing and avoids a failure that would look like a missing object.
	mux.HandleFunc("GET "+p+p+"/subtree_data/{hash}", s.timed("subtree_data", s.getSubtreeData))

	// Everything else — /blocks, /headers_from_common_ancestor, /tx — is a
	// catchup or convenience route the bridge has no data for. 404 is the honest
	// answer and keeps us out of the cluster's malicious-peer classification.
	mux.HandleFunc("/", s.timed("unserved", s.notFound))

	// Trace context arrives on the cluster's fetch, which happens INSIDE its
	// block-validation span. Extracting it here is what keeps "why was this
	// block slow to validate" from dead-ending at the bridge. With tracing
	// disabled the global provider is a no-op and this costs a map lookup per
	// request.
	return otelhttp.NewHandler(mux, "retrieval",
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			return r.Method + " " + r.URL.Path
		}))
}

// timed wraps a handler with the route-latency histogram and the pull-freshness
// stamp. The duration is measured to the point the handler returns, which for
// the streaming routes is after the body has been written — that is the number
// the cluster actually waits on.
func (s *Server) timed(route string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer func() {
			obs.Since(obs.RetrievalDuration, start, route)
			obs.Stamp(obs.LastPullTime)
		}()
		h(w, r)
	}
}

// Serve runs until ctx is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	s.srv = &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      10 * time.Minute, // streaming pulls of a large subtree
		IdleTimeout:       120 * time.Second,
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.cfg.Listen)
	if err != nil {
		return fmt.Errorf("retrieval: listen %s: %w", s.cfg.Listen, err)
	}
	s.bound.Store(true)
	defer s.bound.Store(false)
	s.log.Info("retrieval plane listening", "addr", s.cfg.Listen, "prefix", s.cfg.APIPrefix)

	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(sctx)
	}()

	if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// getSubtree returns the subtree's node hashes: numLeaves × 32 raw bytes, no
// header and no counts. That is precisely the BRC-143 frame with its 40-byte
// header (root + node count) removed, so no transformation is needed.
func (s *Server) getSubtree(w http.ResponseWriter, r *http.Request) {
	h, ok := s.parseHash(w, r)
	if !ok {
		return
	}
	obj, _, found := s.subtrees.Get(cache.Key(h))
	if !found {
		s.miss.Add(1)
		s.log.Warn("subtree pull miss", "hash", h.Display())
		http.Error(w, "unknown subtree", http.StatusNotFound)
		return
	}
	if len(obj) < objfmt.SubtreeHeaderSize {
		s.fail(w, "subtree", h, errors.New("cached frame shorter than its header"))
		return
	}
	nodes := obj[objfmt.SubtreeHeaderSize:]
	s.hitSubtree.Add(1)
	s.firstPull.Pulled(h.Display())
	s.log.Info("served subtree", "hash", h.Display(), "nodes", len(nodes)/32)
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(nodes)
}

// getSubtreeData returns the subtree's member transactions concatenated in node
// order. The coinbase slot is skipped: node 0 of a block's first subtree is the
// all-0xFF placeholder, not a transaction.
func (s *Server) getSubtreeData(w http.ResponseWriter, r *http.Request) {
	h, ok := s.parseHash(w, r)
	if !ok {
		return
	}
	obj, _, found := s.subtrees.Get(cache.Key(h))
	if !found {
		s.miss.Add(1)
		http.Error(w, "unknown subtree", http.StatusNotFound)
		return
	}
	nodes := obj[objfmt.SubtreeHeaderSize:]

	// Collect every member first: a partial body fails the cluster's per-index
	// txid check anyway, and a clean 404 lets it fall back to the batch route.
	out := make([]byte, 0, len(nodes)*8)
	for i := 0; i+32 <= len(nodes); i += 32 {
		if isPlaceholder(nodes[i : i+32]) {
			continue
		}
		var key cache.Key
		copy(key[:], nodes[i:i+32])
		raw, _, ok := s.txs.Get(key)
		if !ok {
			s.miss.Add(1)
			var missing hashid.Hash
			copy(missing[:], nodes[i:i+32])
			s.log.Warn("subtree_data miss: member not held",
				"subtree", h.Display(), "index", i/32, "txid", missing.Display())
			http.Error(w, "member transaction not held", http.StatusNotFound)
			return
		}
		std, err := toStandard(raw)
		if err != nil {
			s.fail(w, "subtree_data", h, err)
			return
		}
		out = append(out, std...)
	}
	s.hitSubtreeData.Add(1)
	s.firstPull.Pulled(h.Display())
	s.log.Info("served subtree_data", "hash", h.Display(), "bytes", len(out))
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(out)
}

// postSubtreeTxs answers the batch route: the body is raw 32-byte txids with no
// count or delimiter, and the response is the matching transactions
// concatenated. The cluster re-keys them by txid, so order does not matter — but
// the count must match exactly, so a single missing member means 404.
func (s *Server) postSubtreeTxs(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		s.fail(w, "txs", hashid.Hash{}, err)
		return
	}
	if len(body)%32 != 0 {
		http.Error(w, "body is not a whole number of 32-byte txids", http.StatusBadRequest)
		return
	}
	out := make([]byte, 0, len(body)*8)
	for i := 0; i+32 <= len(body); i += 32 {
		var key cache.Key
		copy(key[:], body[i:i+32])
		raw, _, ok := s.txs.Get(key)
		if !ok {
			s.miss.Add(1)
			var missing hashid.Hash
			copy(missing[:], body[i:i+32])
			s.log.Warn("txs batch miss", "txid", missing.Display(), "requested", len(body)/32)
			http.Error(w, "transaction not held", http.StatusNotFound)
			return
		}
		std, err := toStandard(raw)
		if err != nil {
			s.fail(w, "txs", hashid.Hash{}, err)
			return
		}
		out = append(out, std...)
	}
	s.hitTxs.Add(1)
	s.log.Info("served txs batch", "count", len(body)/32, "bytes", len(out))
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(out)
}

// getBlock returns the block in Teranode's serialization, transcoded from the
// BRC-144 frame we hold.
func (s *Server) getBlock(w http.ResponseWriter, r *http.Request) {
	h, ok := s.parseHash(w, r)
	if !ok {
		return
	}
	obj, _, found := s.blocks.Get(cache.Key(h))
	if !found {
		s.miss.Add(1)
		s.log.Warn("block pull miss", "hash", h.Display())
		http.Error(w, "unknown block", http.StatusNotFound)
		return
	}
	out, err := tnwire.ToTeranode(obj)
	if err != nil {
		s.fail(w, "block", h, err)
		return
	}
	s.hitBlock.Add(1)
	s.firstPull.Pulled(h.Display())
	s.log.Info("served block", "hash", h.Display(), "bytes", len(out))
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(out)
}

func (s *Server) notFound(w http.ResponseWriter, r *http.Request) {
	// Classify what the cluster asked for. A chain-sync route here is the
	// single strongest signal the deployment can produce: it means the
	// cluster selected the BRIDGE as a catchup source. Rare bursts are the
	// known cached-alternatives path during multi-peer degradation; a
	// SUSTAINED rate means the synthetic peer-id divert has stopped working
	// (id registered, gate semantics changed on upgrade) and the next
	// node to fall behind may wedge. Alert on it; do not ignore it.
	class := "other"
	switch {
	case strings.Contains(r.URL.Path, "/headers_from_common_ancestor") ||
		strings.Contains(r.URL.Path, "/block_locator") ||
		strings.Contains(r.URL.Path, "/blocks"):
		class = "chain_sync"
		s.missChainSync.Add(1)
		s.log.Warn("cluster asked the bridge for a chain-sync route; the bridge is an object source, not a sync peer",
			"method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
	case strings.Contains(r.URL.Path, "/tx"):
		class = "tx"
	}
	s.missRoute.Add(1)
	s.log.Debug("unhandled pull", "method", r.Method, "path", r.URL.Path, "class", class)
	http.Error(w, "not served by this bridge", http.StatusNotFound)
}

func (s *Server) parseHash(w http.ResponseWriter, r *http.Request) (hashid.Hash, bool) {
	h, err := hashid.ParseDisplay(r.PathValue("hash"))
	if err != nil {
		http.Error(w, "bad hash", http.StatusBadRequest)
		return hashid.Hash{}, false
	}
	return h, true
}

// fail answers a genuine server-side fault. Kept deliberately rare: on the block
// path the cluster treats 5xx as recoverable and will redeliver the Kafka
// message indefinitely.
func (s *Server) fail(w http.ResponseWriter, what string, h hashid.Hash, err error) {
	s.errs.Add(1)
	s.log.Error("retrieval failure", "what", what, "hash", h.Display(), "err", err)
	http.Error(w, "bridge error", http.StatusInternalServerError)
}

// toStandard converts an extended-format transaction to standard serialization.
// Teranode's own asset serves non-extended bytes on these routes, and the txid
// is unchanged either way, so matching its behaviour keeps us maximally boring.
func toStandard(raw []byte) ([]byte, error) {
	if !objfmt.IsEF(raw) {
		return raw, nil
	}
	return objfmt.ToStandard(raw)
}

func isPlaceholder(b []byte) bool {
	for _, x := range b {
		if x != 0xFF {
			return false
		}
	}
	return true
}

// Stats is a snapshot for logging and metrics.
type Stats struct {
	Subtree, SubtreeData, Txs, Block, Miss, Errors uint64
	// UnservedRoute counts requests for routes the bridge does not serve at
	// all (vs Miss, which counts known routes for objects it lacks).
	// UnservedChainSync is the subset that asked for chain-sync routes — the
	// canary for the catchup-divert regressing.
	UnservedRoute, UnservedChainSync uint64
}

// Stats returns a snapshot of the retrieval plane's hit, miss and error counts.
func (s *Server) Stats() Stats {
	return Stats{
		Subtree:           s.hitSubtree.Load(),
		SubtreeData:       s.hitSubtreeData.Load(),
		Txs:               s.hitTxs.Load(),
		Block:             s.hitBlock.Load(),
		Miss:              s.miss.Load(),
		Errors:            s.errs.Load(),
		UnservedRoute:     s.missRoute.Load(),
		UnservedChainSync: s.missChainSync.Load(),
	}
}
