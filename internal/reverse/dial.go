package reverse

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

/*
Transport security and liveness for the blockchain connection, mirroring
teranode/util/grpc_helper.go (loadTLSCredentials + the client keepalive block).

# Why TLS is an interop question, not just a hardening one

`security_level_grpc` is a GLOBAL Teranode setting. When it is non-zero,
util.StartGRPCServer wraps EVERY Teranode gRPC listener — blockchain included —
in TLS. A bridge that dials plaintext cannot connect to such a cluster at all,
and the failure is partial in the worst way: the delivery lanes, the announce
path and the retrieval plane keep working, so only the reverse path dies, as an
endless "blockchain subscribe failed, retrying".

# The security levels, verbatim from upstream

	0  insecure, plaintext. Teranode's default.
	1  TLS, and the CLIENT SKIPS VERIFICATION of the server certificate.
	   Upstream's own comment calls this MITM-exploitable by design; it is an
	   operator-opted-in mode for controlled networks, not a secure default.
	2  TLS; the server accepts any client certificate. Client-side this means
	   presenting a certificate AND verifying the server against a CA.
	3  TLS; the server requires and verifies the client certificate. The CLIENT
	   side is identical to level 2 — only the server's ClientAuth differs.

# A limit worth knowing before choosing a level

No Teranode client call site populates CertFile/KeyFile/CaCertFile — only the
SERVER path (util/grpc.go) fills them, from server_certFile/server_keyFile. So at
level 2 or 3 upstream's own inter-service clients hit os.ReadFile("") and fail to
dial. In practice a working cluster runs at level 0 or 1, and level 1 is the one
this matters for. The bridge implements 2 and 3 anyway because it CAN carry cert
paths, but a cluster configured that way has broken inter-service gRPC
independently of the bridge.

# Why there is no retry interceptor

Teranode gives its gRPC clients a retry interceptor; the bridge deliberately
does not adopt it.

It is UNARY-ONLY (grpc.WithChainUnaryInterceptor), so it would never touch the
Subscribe stream that carries the reverse path — the one call whose recovery
actually matters, and which already has its own reconnect loop with exponential
backoff. What is left is the two cluster-state polls, which run on a 15s ticker
under a 5s deadline and record their own failures; retrying inside that deadline
buys nothing the next tick does not.

Upstream's implementation would also import two defects. Its backoff is a bare
time.Sleep with no select on ctx.Done(), so a cancelled call still sleeps the
full ladder; and it retries codes.DeadlineExceeded, which for an EXPIRED CONTEXT
can never succeed — every remaining attempt fails instantly and the sleeps are
pure delay on a doomed call.
*/

// TLSConfig selects the transport security for the blockchain connection,
// mirroring Teranode's security_level_grpc and its certificate paths.
type TLSConfig struct {
	SecurityLevel                 int
	CertFile, KeyFile, CACertFile string
}

// credentials builds the client transport credentials for the configured level.
func (t TLSConfig) credentials() (credentials.TransportCredentials, error) {
	switch t.SecurityLevel {
	case 0:
		return insecure.NewCredentials(), nil

	case 1:
		// Encrypt the channel without verifying the server. Deliberately
		// matching upstream, deliberately loud in the docs: this stops passive
		// eavesdropping and nothing else.
		return credentials.NewTLS(&tls.Config{
			//nolint:gosec // G402: InsecureSkipVerify mirrors upstream's config-gated level 1.
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
		}), nil

	case 2, 3:
		if t.CACertFile == "" || t.CertFile == "" || t.KeyFile == "" {
			return nil, fmt.Errorf("security level %d needs -blockchain-ca-cert, -blockchain-cert and -blockchain-key", t.SecurityLevel)
		}
		caCert, err := os.ReadFile(t.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("read ca cert %s: %w", t.CACertFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			// Upstream ignores this return value, so a PEM with no parseable
			// certificate yields an empty pool and a verification failure at
			// handshake time — a TLS error that reads like a server problem.
			// Fail here instead, where the cause is still the file.
			return nil, fmt.Errorf("ca cert %s contains no usable certificate", t.CACertFile)
		}
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("read key pair: %w", err)
		}
		return credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      pool,
			MinVersion:   tls.VersionTLS12,
		}), nil

	default:
		return nil, fmt.Errorf("unknown security level %d (want 0-3)", t.SecurityLevel)
	}
}

// Keepalive defaults, matching what teranode/util/grpc_helper.go gives every
// client (grpc_keepalive_time_seconds / grpc_keepalive_timeout_seconds).
const (
	DefaultKeepaliveTime    = 30 * time.Second
	DefaultKeepaliveTimeout = 20 * time.Second

	// MinKeepaliveTime is grpc-go's own floor on the client ping interval
	// (internal.KeepaliveMinPingTime). grpc.WithKeepaliveParams SILENTLY raises
	// anything below it and logs a warning through grpc's logger, so a bridge
	// configured with -blockchain-keepalive 5s would not ping at 5s and the only
	// evidence would be a stray line from a library. Clamp here instead, where
	// the bridge can say so in its own log.
	MinKeepaliveTime = 10 * time.Second
)

// keepaliveParams builds the client keepalive policy.
//
// # Why this is load-bearing and not tuning
//
// grpc-go's client keepalive default is INFINITY — a client with no explicit
// policy never pings. The bridge's blockchain connection is a single long-lived
// Subscribe stream that crosses a tunnel, and a silently dropped path
// (WireGuard rekey, conntrack eviction, a firewall losing state) does not close
// the socket. Without pings, stream.Recv() blocks FOREVER: the reconnect loop
// never runs because it is only entered on a Recv error.
//
// The server side does not save us. It pings the client every 60s and would
// eventually tear down its own half, but the client's blocked Recv never learns
// that. Blockchain also sends PING notifications every 10s
// (blockchain_heartbeat_interval), so the wedge is VISIBLE within seconds on
// teranode_bridge_last_notification_timestamp_seconds — but visible is not
// self-healing, and before this the only recovery was a human restarting the
// process.
//
// With the defaults, a dead path is detected in at most Time+Timeout (50s), the
// transport closes, Recv returns, and the existing reconnect loop takes over.
//
// # The constraint that bites
//
// Time must be >= the server's KeepaliveEnforcementPolicy.MinTime
// (grpc_server_min_ping_time_seconds, default 30s). Ping faster than that and
// the server answers GOAWAY ENHANCE_YOUR_CALM "too_many_pings" and drops the
// connection — which the reconnect loop would then retry, turning a liveness
// mechanism into a ping storm. The default here equals upstream's client
// default, so it is safe against a stock cluster; raise it in lockstep if an
// operator raises MinTime.
func keepaliveParams(every, timeout time.Duration, permitWithoutStream bool) (keepalive.ClientParameters, bool) {
	if every <= 0 {
		every = DefaultKeepaliveTime
	}
	if timeout <= 0 {
		timeout = DefaultKeepaliveTimeout
	}
	clamped := false
	if every < MinKeepaliveTime {
		every, clamped = MinKeepaliveTime, true
	}
	return keepalive.ClientParameters{
		Time:                every,
		Timeout:             timeout,
		PermitWithoutStream: permitWithoutStream,
	}, clamped
}
