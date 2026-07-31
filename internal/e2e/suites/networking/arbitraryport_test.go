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
// "<actor-dns-name>:<port>" (see e2e.RouterClient.Connect). Until that support
// lands (cmd/atenet/internal/router/extproc.go's "handle more than port 80"
// TODO and internal/atunnel/client.go's "support/use CONNECT on Ingress"
// TODO), the "arbitrary port reachable" and "unlisted port rejected" subtests
// below are expected to fail; the "primary port unaffected" subtest is a
// regression guard and should already pass.
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
	actorName, actor := createAndResumeActor(t, ctx, "arbport")
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

		wantPort := fmt.Sprintf("extra port %d", counterExtraPort)
		if !strings.Contains(string(body), wantPort) {
			t.Errorf("response body %q does not identify itself as %q", body, wantPort)
		}
		if podIP := actor.GetAteomPodIp(); podIP != "" && !strings.Contains(string(body), podIP) {
			t.Errorf("response body %q does not mention the actor's pod IP %q", body, podIP)
		}
	})

	t.Run("unlisted port rejected", func(t *testing.T) {
		conn, err := router.Connect(ctx, actorRef, counterUnusedPort)
		if err == nil {
			conn.Close()
			t.Fatalf("CONNECT to unlisted actor port %d unexpectedly succeeded", counterUnusedPort)
		}
		t.Logf("CONNECT to unlisted actor port %d was rejected as expected: %v", counterUnusedPort, err)
	})
}
