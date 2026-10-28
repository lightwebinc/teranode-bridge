// Package activation adopts listening sockets passed by systemd socket
// activation (sd_listen_fds(3)): LISTEN_PID names this process, LISTEN_FDS
// counts the sockets, which start at file descriptor 3, and LISTEN_FDNAMES
// carries each socket's FileDescriptorName.
//
// A socket held by systemd stays open across a restart of the service, so a
// peer that connects while the bridge is down waits in the kernel's accept
// queue instead of being refused, and its data waits in the socket buffer.
package activation

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

// listenFdsStart is SD_LISTEN_FDS_START: the first passed descriptor.
const listenFdsStart = 3

// Listeners returns the sockets systemd passed to this process, by
// FileDescriptorName (unnamed sockets get "fd<N>"). It returns an empty map
// when the process was not socket-activated, and unsets the variables so a
// child process does not adopt the same sockets.
func Listeners() (map[string]net.Listener, error) {
	defer func() {
		_ = os.Unsetenv("LISTEN_PID")
		_ = os.Unsetenv("LISTEN_FDS")
		_ = os.Unsetenv("LISTEN_FDNAMES")
	}()
	return fromEnv(os.Getenv, os.Getpid(), listenFdsStart)
}

// fromEnv is Listeners over an explicit environment, process id and first
// descriptor, for tests.
func fromEnv(getenv func(string) string, pid, start int) (map[string]net.Listener, error) {
	out := map[string]net.Listener{}
	if getenv("LISTEN_PID") == "" {
		return out, nil
	}
	if p, err := strconv.Atoi(getenv("LISTEN_PID")); err != nil || p != pid {
		return out, nil // meant for another process
	}
	n, err := strconv.Atoi(getenv("LISTEN_FDS"))
	if err != nil || n < 0 {
		return out, fmt.Errorf("activation: LISTEN_FDS %q", getenv("LISTEN_FDS"))
	}
	var names []string
	if v := getenv("LISTEN_FDNAMES"); v != "" {
		names = strings.Split(v, ":")
	}
	for i := 0; i < n; i++ {
		fd := start + i
		name := fmt.Sprintf("fd%d", fd)
		if i < len(names) && names[i] != "" {
			name = names[i]
		}
		f := os.NewFile(uintptr(fd), name)
		l, err := net.FileListener(f)
		_ = f.Close() // FileListener holds its own duplicate
		if err != nil {
			return out, fmt.Errorf("activation: socket %s (fd %d): %w", name, fd, err)
		}
		out[name] = l
	}
	return out, nil
}
