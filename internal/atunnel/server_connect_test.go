// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file defines the acceptance contract for atunnel's ingress side of
// arbitrary-port routing: a CONNECT request whose :authority carries the
// target port (mirroring the egress Client's CONNECT dialing in client.go)
// should tunnel raw bytes to that port on the actor, on the same host as the
// Server's configured Upstream, rather than always reverse-proxying to
// Upstream's fixed port. See client.go's "support/use CONNECT on Ingress as
// well" TODO. TestConnectToArbitraryPort is expected to fail until that
// support exists; TestConnectRejectsInactiveActor already passes today since
// it only exercises the existing activation check.
package atunnel

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"
)

// newRealServer builds a Server with its own CA, trusted client cert, and
// server credential bundle -- the same setup TestMutualTLSClientIdentity uses
// -- so a real TLS client can complete a handshake, unlike newTestServer whose
// implicit CA never hands out a usable client certificate.
func newRealServer(t *testing.T, upstream *url.URL) (*Server, tls.Certificate) {
	t.Helper()
	dir := t.TempDir()
	ca := newTestCA(t)
	serverCert := ca.issue(t, "", []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	bundlePath := filepath.Join(dir, "server.pem")
	trustPath := filepath.Join(dir, "trust.pem")
	writeCredentialBundle(t, bundlePath, serverCert)
	if err := os.WriteFile(trustPath, ca.certPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	const clientID = "spiffe://cluster.local/ns/ate-system/sa/atenet-router"
	s, err := NewServer(Config{
		CredentialBundlePath: bundlePath,
		TrustBundlePath:      trustPath,
		AllowedClientID:      clientID,
		Upstream:             upstream,
	})
	if err != nil {
		t.Fatal(err)
	}
	clientCert := ca.issue(t, clientID, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	return s, clientCert
}

// serveOnListener runs s on a real loopback listener (httptest.NewRecorder
// can't Hijack, which a CONNECT tunnel requires) and returns its address.
func serveOnListener(t *testing.T, s *Server) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := s.Serve(ctx, lis); err != nil {
			t.Errorf("Serve: %v", err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return lis.Addr().String()
}

// startFakeActor stands in for a second port on the actor: on every
// connection it writes banner once, then echoes further bytes back.
func startFakeActor(t *testing.T, banner string) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lis.Close() })
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				if _, err := io.WriteString(conn, banner); err != nil {
					return
				}
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return lis.Addr().String()
}

// connectThroughServer dials addr with clientCert and sends a CONNECT request
// for authority, returning the tunnel connection and the CONNECT response.
func connectThroughServer(t *testing.T, addr string, clientCert tls.Certificate, authority string) (net.Conn, *http.Response) {
	t.Helper()
	conn, err := tls.Dial("tcp", addr, &tls.Config{
		InsecureSkipVerify: true, // Test only the tunnel, not server identity pinning.
		Certificates:       []tls.Certificate{clientCert},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Host: authority},
		Host:   authority,
	}
	if err := req.Write(conn); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, req)
	if err != nil {
		t.Fatal(err)
	}
	return &bufferedConn{Conn: conn, reader: reader}, resp
}

func TestConnectToArbitraryPort(t *testing.T) {
	const banner = "hello from the arbitrary port\n"
	fakeActorAddr := startFakeActor(t, banner)
	_, fakeActorPort, err := net.SplitHostPort(fakeActorAddr)
	if err != nil {
		t.Fatal(err)
	}

	// The requested port is irrelevant to the fixed reverse proxy this points
	// at today; once CONNECT support lands it should be swapped in per-request
	// against the same host.
	upstream, err := url.Parse("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	s, clientCert := newRealServer(t, upstream)
	if err := s.Activate("team-a", "actor-1"); err != nil {
		t.Fatal(err)
	}
	addr := serveOnListener(t, s)

	authority := fmt.Sprintf("actor-1.team-a.actors.resources.substrate.ate.dev:%s", fakeActorPort)
	conn, resp := connectThroughServer(t, addr, clientCert, authority)
	defer conn.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT to actor's arbitrary port %s returned %s, want 200", fakeActorPort, resp.Status)
	}

	got := make([]byte, len(banner))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("reading banner through tunnel: %v", err)
	}
	if string(got) != banner {
		t.Errorf("banner through tunnel = %q, want %q", got, banner)
	}
}

func TestConnectRejectsInactiveActor(t *testing.T) {
	fakeActorAddr := startFakeActor(t, "unused\n")
	_, fakeActorPort, err := net.SplitHostPort(fakeActorAddr)
	if err != nil {
		t.Fatal(err)
	}

	upstream, err := url.Parse("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	s, clientCert := newRealServer(t, upstream)
	// Deliberately not Activate'd.
	addr := serveOnListener(t, s)

	authority := fmt.Sprintf("actor-1.team-a.actors.resources.substrate.ate.dev:%s", fakeActorPort)
	conn, resp := connectThroughServer(t, addr, clientCert, authority)
	defer conn.Close()
	if resp.StatusCode != http.StatusMisdirectedRequest {
		t.Fatalf("CONNECT for an inactive actor returned %s, want %d", resp.Status, http.StatusMisdirectedRequest)
	}
}

func TestConnectToClosedPortFails(t *testing.T) {
	// Bind then immediately close a listener, so its port is (almost
	// certainly) free but nothing is listening on it.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_, closedPort, err := net.SplitHostPort(lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	lis.Close()

	upstream, err := url.Parse("http://127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	s, clientCert := newRealServer(t, upstream)
	if err := s.Activate("team-a", "actor-1"); err != nil {
		t.Fatal(err)
	}
	addr := serveOnListener(t, s)

	authority := fmt.Sprintf("actor-1.team-a.actors.resources.substrate.ate.dev:%s", closedPort)
	conn, resp := connectThroughServer(t, addr, clientCert, authority)
	defer conn.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		t.Fatalf("CONNECT to a closed port returned %s, want a non-2xx status", resp.Status)
	}
}
