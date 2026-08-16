// Package announce publishes subtree and block announcements into the Teranode
// cluster's Kafka, pointing them at this bridge's retrieval plane.
//
// This is the whole trick of the bridge. Teranode learns of a subtree or block
// as {hash, URL} and fetches the bytes from that URL; by announcing ourselves as
// the source for objects we already hold, the wide-area transfer becomes a push
// while the cluster keeps its ordinary announce→fetch→validate path unchanged.
//
// The two messages are protobuf with three string fields each (hash=1, URL=2,
// peer_id=3). They are encoded here with the canonical low-level wire encoder
// rather than generated stubs: pulling in the cluster's generated package would
// drag its entire module into a small landing-tier binary.
package announce

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protowire"
)

// Message is the common shape of KafkaSubtreeTopicMessage and
// KafkaBlockTopicMessage.
type Message struct {
	Hash   string // object hash, hex, display order
	URL    string // base URL of the server that will serve the bytes
	PeerID string // originating peer identity, informational to the consumer
}

// Encode returns the protobuf wire encoding. Fields are written in ascending
// number order and empty strings are omitted, which is what proto3's canonical
// encoder does — an explicitly-encoded empty field is legal but non-canonical,
// and a PeerID left empty against guidance is omitted entirely (see
// Config.PeerID — real deployments must set a synthetic id).
func (m Message) Encode() []byte {
	var b []byte
	if m.Hash != "" {
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendString(b, m.Hash)
	}
	if m.URL != "" {
		b = protowire.AppendTag(b, 2, protowire.BytesType)
		b = protowire.AppendString(b, m.URL)
	}
	if m.PeerID != "" {
		b = protowire.AppendTag(b, 3, protowire.BytesType)
		b = protowire.AppendString(b, m.PeerID)
	}
	return b
}

// Decode parses the wire form back into a Message. Only used by tests and by
// tooling that inspects what we produced.
func Decode(b []byte) (Message, error) {
	var m Message
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return m, protowire.ParseError(n)
		}
		b = b[n:]
		if typ != protowire.BytesType {
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return m, protowire.ParseError(n)
			}
			b = b[n:]
			continue
		}
		v, n := protowire.ConsumeString(b)
		if n < 0 {
			return m, protowire.ParseError(n)
		}
		b = b[n:]
		switch num {
		case 1:
			m.Hash = v
		case 2:
			m.URL = v
		case 3:
			m.PeerID = v
		}
	}
	return m, nil
}

// Config describes the cluster's Kafka and the topics to announce on.
type Config struct {
	Brokers      []string
	SubtreeTopic string
	BlockTopic   string

	// PeerID must be a SYNTHETIC libp2p peer id: a valid-format (12D3KooW…)
	// identity derived from a fresh key and registered with no p2p service.
	// The cluster's catchup gate treats an unregistered id as unhealthy and
	// diverts chain sync to real libp2p peers, while the delivery gates only
	// check bans and keep pulling objects from the bridge — so the bridge
	// augments the network without ever becoming a sync source. EMPTY IS
	// UNSAFE: catchup substitutes the announce URL for a missing id, targets
	// the bridge's retrieval plane for the header chain, 404s, and
	// circuit-breaks the cluster out of recovery. (Semantics verified at
	// teranode 1cca625; btb_retrieval_unserved_route_total{class="chain_sync"}
	// is the canary that the divert still holds.)
	PeerID string

	Timeout time.Duration // per-produce deadline
}

// Producer publishes announcements.
type Producer struct {
	cfg    Config
	client *kgo.Client
	log    *slog.Logger

	subtrees, blocks, failures atomic.Uint64
}

// New connects to the cluster's Kafka.
func New(cfg Config, log *slog.Logger) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("announce: no brokers configured")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 10 * time.Second
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("teranode-bridge"),
		// The consumers are ordinary Kafka consumers; nothing here needs
		// transactions or idempotent producer semantics — a duplicate announce
		// is harmless because the cluster dedups by hash.
		kgo.ProducerBatchMaxBytes(16<<20),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("announce: kafka client: %w", err)
	}
	return &Producer{cfg: cfg, client: cl, log: log}, nil
}

// Ping verifies the brokers are reachable and the advertised listener resolves
// from here — a misadvertised listener otherwise fails only at produce time.
func (p *Producer) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()
	return p.client.Ping(ctx)
}

// Subtree announces a subtree, telling the cluster to fetch it from baseURL.
func (p *Producer) Subtree(ctx context.Context, hash, baseURL string) error {
	err := p.produce(ctx, p.cfg.SubtreeTopic, Message{Hash: hash, URL: baseURL, PeerID: p.cfg.PeerID})
	if err != nil {
		p.failures.Add(1)
		return err
	}
	p.subtrees.Add(1)
	return nil
}

// Block announces a block, telling the cluster to fetch it from baseURL.
func (p *Producer) Block(ctx context.Context, hash, baseURL string) error {
	err := p.produce(ctx, p.cfg.BlockTopic, Message{Hash: hash, URL: baseURL, PeerID: p.cfg.PeerID})
	if err != nil {
		p.failures.Add(1)
		return err
	}
	p.blocks.Add(1)
	return nil
}

func (p *Producer) produce(ctx context.Context, topic string, m Message) error {
	if topic == "" {
		return errors.New("announce: empty topic")
	}
	ctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()

	rec := &kgo.Record{Topic: topic, Value: m.Encode()}
	if err := p.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("announce: produce to %s: %w", topic, err)
	}
	p.log.Debug("announced", "topic", topic, "hash", m.Hash, "url", m.URL)
	return nil
}

// Stats is a snapshot for logging and metrics.
type Stats struct{ Subtrees, Blocks, Failures uint64 }

// Stats returns a snapshot of the announcement counters.
func (p *Producer) Stats() Stats {
	return Stats{Subtrees: p.subtrees.Load(), Blocks: p.blocks.Load(), Failures: p.failures.Load()}
}

// Close shuts the Kafka client down, flushing any in-flight produce.
func (p *Producer) Close() { p.client.Close() }
