package activation

import (
	"net"
	"syscall"
	"testing"
)

// fromEnv adopts passed sockets by name, starting at the given descriptor, and
// only when LISTEN_PID names this process.
func TestFromEnvAdoptsNamedSockets(t *testing.T) {
	l1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l1.Close() }()
	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l2.Close() }()

	// Place duplicates of both sockets on two consecutive descriptors, as
	// systemd would from descriptor 3.
	f1, _ := l1.(*net.TCPListener).File()
	f2, _ := l2.(*net.TCPListener).File()
	defer func() { _ = f1.Close(); _ = f2.Close() }()
	start, err := syscall.Dup(int(f1.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Dup2(int(f2.Fd()), start+1); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"LISTEN_PID": "42", "LISTEN_FDS": "2", "LISTEN_FDNAMES": "tx:subtree"}

	got, err := fromEnv(func(k string) string { return env[k] }, 42, start)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got["tx"] == nil || got["subtree"] == nil {
		t.Fatalf("adopted %v, want tx and subtree", got)
	}
	if got["tx"].Addr().String() != l1.Addr().String() || got["subtree"].Addr().String() != l2.Addr().String() {
		t.Fatal("sockets adopted under the wrong names")
	}
	for _, l := range got {
		_ = l.Close()
	}

	none, err := fromEnv(func(k string) string { return env[k] }, 7, start)
	if err != nil || len(none) != 0 {
		t.Fatalf("sockets for another process adopted: %v %v", none, err)
	}
}
