package obs

import (
	"net"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Kafka producer instrumentation, mirroring teranode/util/kafka (metrics.go +
// producer_hooks.go) metric-for-metric. Both sides use franz-go, so the same
// hook interfaces carry the same measurements; only the namespace differs
// (`teranode_bridge_kafka_producer_*` rather than `teranode_kafka_producer_*`,
// so a bridge and the cluster it fronts stay distinguishable on one dashboard).
//
// Without these, an announce BACKLOG is invisible: `announce_failures_total`
// only moves once produce actually fails, while a producer that is merely
// falling behind shows nothing at all — and a subtree the cluster is never told
// about is exactly as lost as one that failed to send.
const kafkaSubsystem = "bridge_kafka_producer"

func kafkaHist(name, help string, buckets []float64) prometheus.Histogram {
	return prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: Namespace, Subsystem: kafkaSubsystem,
		Name: name, Help: help, Buckets: buckets,
	})
}

func kafkaCounter(name, help string) prometheus.Counter {
	return prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: Namespace, Subsystem: kafkaSubsystem, Name: name, Help: help,
	})
}

var (
	kafkaBytesWritten = kafkaCounter("bytes_written_total",
		"Bytes written to Kafka brokers.")
	kafkaWriteDuration = kafkaHist("write_duration_seconds",
		"Time spent in conn.Write for a produce request.", BucketsMilliSeconds)
	kafkaWriteErrors = kafkaCounter("write_errors_total",
		"Broker write errors encountered during produce.")
	kafkaE2EDuration = kafkaHist("e2e_duration_seconds",
		"End-to-end time from writing a produce request to reading its response.", BucketsMilliLongSeconds)
	kafkaProduceLatency = kafkaHist("produce_request_latency_seconds",
		"Produce request latency including the wait before the write.", BucketsMilliLongSeconds)
	kafkaBatchRecords = kafkaCounter("batch_records_total",
		"Records successfully produced in batches.")
	kafkaBatchCompressed = kafkaCounter("batch_compressed_bytes_total",
		"Compressed bytes of successfully produced batches.")
	kafkaBrokerConnects = kafkaCounter("broker_connects_total",
		"Successful broker connections opened.")
	kafkaBrokerDisconnects = kafkaCounter("broker_disconnects_total",
		"Broker connections closed. A climbing rate with a flat produce rate means the brokers are cycling us.")
	kafkaConnectErrors = kafkaCounter("connect_errors_total",
		"Failed broker dials.")
)

func kafkaCollectors() prometheus.Collector { return kafkaSet{} }

// kafkaSet bundles the producer metrics as one collector so [Collectors] stays
// a flat list.
type kafkaSet struct{}

func (kafkaSet) each() []prometheus.Collector {
	return []prometheus.Collector{
		kafkaBytesWritten, kafkaWriteDuration, kafkaWriteErrors,
		kafkaE2EDuration, kafkaProduceLatency, kafkaBatchRecords,
		kafkaBatchCompressed, kafkaBrokerConnects, kafkaBrokerDisconnects,
		kafkaConnectErrors,
	}
}

func (k kafkaSet) Describe(ch chan<- *prometheus.Desc) {
	for _, c := range k.each() {
		c.Describe(ch)
	}
}

func (k kafkaSet) Collect(ch chan<- prometheus.Metric) {
	for _, c := range k.each() {
		c.Collect(ch)
	}
}

// produceAPIKey is the Kafka protocol API key for Produce requests. The write
// and e2e hooks fire for every request type the client makes — metadata,
// ApiVersions, heartbeats — and folding those into a "produce latency"
// histogram would quietly bias it downward.
const produceAPIKey int16 = 0

// KafkaHook implements the franz-go hook interfaces that carry produce-path
// measurements:
//
//   - kgo.HookBrokerConnect
//   - kgo.HookBrokerDisconnect
//   - kgo.HookBrokerWrite
//   - kgo.HookBrokerE2E
//   - kgo.HookProduceBatchWritten
type KafkaHook struct{}

// NewKafkaHook returns the hook to pass to kgo.WithHooks.
func NewKafkaHook() *KafkaHook { return &KafkaHook{} }

// OnBrokerConnect implements kgo.HookBrokerConnect.
func (*KafkaHook) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err != nil {
		kafkaConnectErrors.Inc()
		return
	}
	kafkaBrokerConnects.Inc()
}

// OnBrokerDisconnect implements kgo.HookBrokerDisconnect.
func (*KafkaHook) OnBrokerDisconnect(_ kgo.BrokerMetadata, _ net.Conn) {
	kafkaBrokerDisconnects.Inc()
}

// OnBrokerWrite implements kgo.HookBrokerWrite.
func (*KafkaHook) OnBrokerWrite(_ kgo.BrokerMetadata, key int16, bytesWritten int,
	_, timeToWrite time.Duration, err error) {

	if key != produceAPIKey {
		return
	}
	kafkaBytesWritten.Add(float64(bytesWritten))
	kafkaWriteDuration.Observe(timeToWrite.Seconds())
	if err != nil {
		kafkaWriteErrors.Inc()
	}
}

// OnBrokerE2E implements kgo.HookBrokerE2E.
func (*KafkaHook) OnBrokerE2E(_ kgo.BrokerMetadata, key int16, e2e kgo.BrokerE2E) {
	if key != produceAPIKey {
		return
	}
	kafkaE2EDuration.Observe(e2e.DurationE2E().Seconds())
	kafkaProduceLatency.Observe((e2e.WriteWait + e2e.DurationE2E()).Seconds())
	if e2e.Err() != nil {
		kafkaWriteErrors.Inc()
	}
}

// OnProduceBatchWritten implements kgo.HookProduceBatchWritten.
func (*KafkaHook) OnProduceBatchWritten(_ kgo.BrokerMetadata, _ string, _ int32, m kgo.ProduceBatchMetrics) {
	kafkaBatchRecords.Add(float64(m.NumRecords))
	kafkaBatchCompressed.Add(float64(m.CompressedBytes))
}

var (
	_ kgo.HookBrokerConnect       = (*KafkaHook)(nil)
	_ kgo.HookBrokerDisconnect    = (*KafkaHook)(nil)
	_ kgo.HookBrokerWrite         = (*KafkaHook)(nil)
	_ kgo.HookBrokerE2E           = (*KafkaHook)(nil)
	_ kgo.HookProduceBatchWritten = (*KafkaHook)(nil)
)
