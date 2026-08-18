package reverse

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/lightwebinc/teranode-bridge/internal/registry"
	pb "github.com/lightwebinc/teranode-bridge/proto/blockchain_api"
)

// writeCertPair writes a self-signed cert + key and returns their paths.
func writeCertPair(t *testing.T, dir, name string) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("cert: %v", err)
	}
	certPath = filepath.Join(dir, name+".crt")
	keyPath = filepath.Join(dir, name+".key")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	kb, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certPath, keyPath
}

// TestSecurityLevels pins the mapping to Teranode's security_level_grpc. Getting
// this wrong is not a hardening miss but an interop failure: security_level_grpc
// is global cluster-side, so a mismatched level means the reverse path can never
// connect while every other plane keeps working.
func TestSecurityLevels(t *testing.T) {
	dir := t.TempDir()
	cert, key := writeCertPair(t, dir, "client")
	ca, _ := writeCertPair(t, dir, "ca")

	t.Run("0 is plaintext", func(t *testing.T) {
		c, err := TLSConfig{SecurityLevel: 0}.credentials()
		if err != nil {
			t.Fatalf("level 0: %v", err)
		}
		if c.Info().SecurityProtocol != insecure.NewCredentials().Info().SecurityProtocol {
			t.Errorf("level 0 is not insecure: %q", c.Info().SecurityProtocol)
		}
	})

	t.Run("1 needs no files", func(t *testing.T) {
		c, err := TLSConfig{SecurityLevel: 1}.credentials()
		if err != nil {
			t.Fatalf("level 1: %v", err)
		}
		if c.Info().SecurityProtocol != "tls" {
			t.Errorf("level 1 is not tls: %q", c.Info().SecurityProtocol)
		}
	})

	for _, lvl := range []int{2, 3} {
		t.Run("client side of "+string(rune('0'+lvl)), func(t *testing.T) {
			c, err := TLSConfig{SecurityLevel: lvl, CACertFile: ca, CertFile: cert, KeyFile: key}.credentials()
			if err != nil {
				t.Fatalf("level %d: %v", lvl, err)
			}
			if c.Info().SecurityProtocol != "tls" {
				t.Errorf("level %d is not tls: %q", lvl, c.Info().SecurityProtocol)
			}
		})
	}
}

// TestSecurityLevelMisconfigurationFailsAtDial pins that a bad level or missing
// file is refused when the bridge starts, not at handshake time. A TLS error
// surfacing later reads like a cluster fault; refusing here names the flag.
func TestSecurityLevelMisconfigurationFailsAtDial(t *testing.T) {
	dir := t.TempDir()
	cert, key := writeCertPair(t, dir, "client")

	cases := []struct {
		name string
		cfg  TLSConfig
		want string
	}{
		{"unknown level", TLSConfig{SecurityLevel: 4}, "unknown security level"},
		{"negative level", TLSConfig{SecurityLevel: -1}, "unknown security level"},
		{"level 2 without files", TLSConfig{SecurityLevel: 2}, "-blockchain-ca-cert"},
		{"level 3 without ca", TLSConfig{SecurityLevel: 3, CertFile: cert, KeyFile: key}, "-blockchain-ca-cert"},
		{"missing ca file", TLSConfig{SecurityLevel: 2, CACertFile: filepath.Join(dir, "nope.crt"), CertFile: cert, KeyFile: key}, "read ca cert"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.cfg.credentials()
			if err == nil {
				t.Fatal("misconfiguration accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// TestUnusableCAIsRefused guards a defect inherited from upstream, which ignores
// AppendCertsFromPEM's return value: a PEM with no parseable certificate yields
// an EMPTY pool, and the failure surfaces later as a handshake verification
// error that reads like a bad server certificate.
func TestUnusableCAIsRefused(t *testing.T) {
	dir := t.TempDir()
	cert, key := writeCertPair(t, dir, "client")
	junk := filepath.Join(dir, "junk.crt")
	if err := os.WriteFile(junk, []byte("-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := TLSConfig{SecurityLevel: 2, CACertFile: junk, CertFile: cert, KeyFile: key}.credentials()
	if err == nil {
		t.Fatal("a CA file with no usable certificate was accepted")
	}
	if !strings.Contains(err.Error(), "no usable certificate") {
		t.Errorf("error %q does not name the cause", err)
	}
}

// TestKeepaliveDefaultsAreNotGrpcDefaults is the point of the whole keepalive
// change: grpc-go's client default is INFINITY, so an unconfigured client never
// pings and a silently dropped path leaves Recv blocked forever. Zero here must
// mean "upstream's client default", never "grpc-go's".
func TestKeepaliveDefaultsAreNotGrpcDefaults(t *testing.T) {
	kp, clamped := keepaliveParams(0, 0, true)
	if clamped {
		t.Error("defaults reported as clamped")
	}
	if kp.Time != DefaultKeepaliveTime {
		t.Errorf("keepalive Time %v, want %v", kp.Time, DefaultKeepaliveTime)
	}
	if kp.Timeout != DefaultKeepaliveTimeout {
		t.Errorf("keepalive Timeout %v, want %v", kp.Timeout, DefaultKeepaliveTimeout)
	}
	if !kp.PermitWithoutStream {
		t.Error("PermitWithoutStream not carried through")
	}

	// The constraint that bites: pinging faster than the cluster's
	// grpc_server_min_ping_time_seconds (default 30s) earns a GOAWAY
	// too_many_pings, and the reconnect loop turns that into a ping storm.
	if DefaultKeepaliveTime < 30*time.Second {
		t.Fatalf("default keepalive %v pings faster than the stock cluster's 30s MinTime",
			DefaultKeepaliveTime)
	}

	kp, _ = keepaliveParams(45*time.Second, 5*time.Second, false)
	if kp.Time != 45*time.Second || kp.Timeout != 5*time.Second || kp.PermitWithoutStream {
		t.Errorf("explicit values not honoured: %+v", kp)
	}
}

// TestKeepaliveBelowGrpcFloorIsReported guards a silent override: grpc-go clamps
// any client ping interval below 10s and logs the adjustment through its OWN
// logger, so a bridge asked for 5s would not ping at 5s and the only trace would
// be a stray library line in a different format. The bridge clamps first and
// says so itself.
func TestKeepaliveBelowGrpcFloorIsReported(t *testing.T) {
	kp, clamped := keepaliveParams(5*time.Second, time.Second, true)
	if !clamped {
		t.Fatal("a sub-floor interval was accepted silently")
	}
	if kp.Time != MinKeepaliveTime {
		t.Errorf("clamped to %v, want %v", kp.Time, MinKeepaliveTime)
	}
}

// --- the actual failure mode: a silently dead path ---------------------------

// blackhole is a TCP relay that can stop forwarding WITHOUT closing either
// socket — the shape of a WireGuard rekey, a conntrack eviction or a firewall
// losing state. This is what makes the bug invisible: nothing errors, the
// connection just stops carrying bytes.
type blackhole struct {
	ln      net.Listener
	backend string
	dropped atomic.Bool
	conns   sync.Map
}

func newBlackhole(t *testing.T, backend string) *blackhole {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("blackhole listen: %v", err)
	}
	b := &blackhole{ln: ln, backend: backend}
	go b.serve()
	t.Cleanup(func() { _ = ln.Close() })
	return b
}

func (b *blackhole) addr() string { return b.ln.Addr().String() }

// drop stops forwarding in both directions. Sockets stay open on purpose.
func (b *blackhole) drop() { b.dropped.Store(true) }

func (b *blackhole) serve() {
	for {
		in, err := b.ln.Accept()
		if err != nil {
			return
		}
		out, err := net.Dial("tcp", b.backend)
		if err != nil {
			_ = in.Close()
			continue
		}
		b.conns.Store(in, out)
		go b.pump(in, out)
		go b.pump(out, in)
	}
}

func (b *blackhole) pump(src, dst net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 && !b.dropped.Load() {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// startSilentBlockchain runs a real BlockchainAPI server that accepts a
// Subscribe stream and then sends nothing — an idle cluster.
func startSilentBlockchain(t *testing.T) (addr string, opened chan struct{}) {
	t.Helper()
	srv := &silentServer{opened: make(chan struct{}, 1), stop: make(chan struct{})}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	g := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		// Match a stock cluster: grpc_server_min_ping_time_seconds default 30s.
		MinTime:             30 * time.Second,
		PermitWithoutStream: true,
	}))
	pb.RegisterBlockchainAPIServer(g, srv)
	go func() { _ = g.Serve(ln) }()
	t.Cleanup(func() { close(srv.stop); g.Stop() })
	return ln.Addr().String(), srv.opened
}

type silentServer struct {
	pb.UnimplementedBlockchainAPIServer
	opened chan struct{}
	stop   chan struct{}
}

func (s *silentServer) Subscribe(_ *pb.SubscribeRequest, _ grpc.ServerStreamingServer[pb.Notification]) error {
	select {
	case s.opened <- struct{}{}:
	default:
	}
	<-s.stop
	return nil
}

func runSubscriber(t *testing.T, addr string, ka, kaTimeout time.Duration) *Subscriber {
	t.Helper()
	sub, err := New(Config{
		BlockchainAddr:      addr,
		KeepaliveTime:       ka,
		KeepaliveTimeout:    kaTimeout,
		PermitWithoutStream: true,
	}, registry.New(time.Minute, 16), nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); _ = sub.Close() })
	go func() { _ = sub.Run(ctx) }()
	return sub
}

func waitReconnect(sub *Subscriber, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if sub.Stats().Reconnects > 0 {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// TestKeepaliveRecoversASilentlyDeadPath is the whole point of the change.
//
// The path is blackholed mid-stream without either socket closing, which is what
// a tunnel rekey or a conntrack eviction looks like. Nothing errors on its own:
// the server's own keepalive eventually gives up its half, but the CLIENT is
// parked in stream.Recv() and the reconnect loop is only entered on a Recv
// error. Client pings are the only thing that turns this into a detected fault.
func TestKeepaliveRecoversASilentlyDeadPath(t *testing.T) {
	if testing.Short() {
		t.Skip("takes ~15s: grpc-go clamps client pings to a 10s floor")
	}
	backend, opened := startSilentBlockchain(t)
	bh := newBlackhole(t, backend)

	// The floor, because grpc-go will not ping faster whatever we ask for.
	sub := runSubscriber(t, bh.addr(), MinKeepaliveTime, time.Second)

	select {
	case <-opened:
	case <-time.After(10 * time.Second):
		t.Fatal("stream never opened")
	}
	bh.drop()

	if !waitReconnect(sub, 25*time.Second) {
		t.Fatal("the bridge never noticed a silently dead path — Recv is wedged, " +
			"and before keepalive that state persisted until someone restarted the process")
	}
}

// TestWithoutKeepaliveTheBridgeWedges is the control, and the reason the change
// exists: same blackhole, pings effectively off, and the bridge sits there.
func TestWithoutKeepaliveTheBridgeWedges(t *testing.T) {
	if testing.Short() {
		t.Skip("takes ~15s")
	}
	backend, opened := startSilentBlockchain(t)
	bh := newBlackhole(t, backend)

	sub := runSubscriber(t, bh.addr(), time.Hour, time.Hour)

	select {
	case <-opened:
	case <-time.After(10 * time.Second):
		t.Fatal("stream never opened")
	}
	bh.drop()

	if waitReconnect(sub, 15*time.Second) {
		t.Fatal("reconnected without keepalive; the control assumption is wrong " +
			"and the detection test above proves nothing")
	}
}
