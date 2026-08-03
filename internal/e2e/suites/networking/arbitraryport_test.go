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

// This file defines the acceptance contract for atenet-router's arbitrary-port
// ingress support: a client reaches a port other than the actor's primary one
// by opening an HTTP CONNECT tunnel through the router whose :authority is
// "<actor-dns-name>:<port>" (see e2e.RouterClient.Connect). Supported for
// HTTP/1.1 and h2c payloads tunneled over CONNECT, whether the CONNECT itself
// is plaintext or TLS-wrapped. CONNECT succeeds as soon as the actor resolves,
// independent of whether the target port is actually reachable -- a request
// sent over the tunnel to an unlisted port fails at atunnel's reverse proxy
// (502), not at CONNECT accept time (see "unlisted port rejected" below).
package networking

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/internal/resources"
)

// counterExtraPort is the second, arbitrary port the counter demo listens on
// (see demos/counter/counter.go's --extra-port and counter.yaml.tmpl).
const counterExtraPort = 9090

// counterUnusedPort is a port nothing in the counter demo listens on, used to
// prove the tunnel doesn't blindly succeed.
const counterUnusedPort = 6553

func TestActorArbitraryPortAccess(t *testing.T) {
	ctx := context.Background()
	actorName, _ := createAndResumeActor(t, ctx, "arbport")
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	router := mustRouterClient(t, ctx)
	defer router.Close()

	t.Run("primary port unaffected", func(t *testing.T) {
		response, err := router.Get(ctx, actorRef, "/readyz")
		if err != nil {
			t.Fatalf("GET Actor through ingress: %v", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(response.Body)
			t.Fatalf("Actor access through ingress returned HTTP %d, want 200; body: %s", response.StatusCode, body)
		}
	})

	t.Run("arbitrary port reachable", func(t *testing.T) {
		conn, err := router.Connect(ctx, actorRef, counterExtraPort)
		if err != nil {
			t.Fatalf("CONNECT to actor port %d through ingress: %v", counterExtraPort, err)
		}
		defer conn.Close()

		if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: "+actorRef.DNSName()+"\r\nConnection: close\r\n\r\n"); err != nil {
			t.Fatalf("writing request over CONNECT tunnel: %v", err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatalf("reading response over CONNECT tunnel: %v", err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("reading response body over CONNECT tunnel: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("request over CONNECT tunnel returned HTTP %d; body: %s", resp.StatusCode, body)
		}

		// counter.go's getCurrentIP() reports the first non-loopback address
		// it finds inside the gVisor sandbox's network namespace (a fixed
		// link-local address), never the actor's real k8s pod IP -- so the
		// response body has no pod IP to check here. The extra-port marker
		// is what proves the tunnel reached the right port.
		wantPort := fmt.Sprintf("extra port %d", counterExtraPort)
		if !strings.Contains(string(body), wantPort) {
			t.Errorf("response body %q does not identify itself as %q", body, wantPort)
		}
	})

	t.Run("unlisted port rejected", func(t *testing.T) {
		// The CONNECT itself succeeds unconditionally once the actor
		// resolves -- the router never checks the target port's reachability
		// at accept time. The rejection instead comes from atunnel's reverse
		// proxy failing to dial the (unlisted) port for an actual request
		// sent over the tunnel.
		conn, err := router.Connect(ctx, actorRef, counterUnusedPort)
		if err != nil {
			t.Fatalf("CONNECT to actor port %d through ingress: %v", counterUnusedPort, err)
		}
		defer conn.Close()

		if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: "+actorRef.DNSName()+"\r\nConnection: close\r\n\r\n"); err != nil {
			t.Fatalf("writing request over CONNECT tunnel: %v", err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatalf("reading response over CONNECT tunnel: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("request to unlisted actor port %d returned HTTP %d, want %d; body: %s", counterUnusedPort, resp.StatusCode, http.StatusBadGateway, body)
		}
		t.Logf("request to unlisted actor port %d was rejected as expected: HTTP %d", counterUnusedPort, resp.StatusCode)
	})
}
